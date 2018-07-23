#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018


import commands
import os

from pilot.control.payloads import generic
from pilot.util.auxiliary import get_logger

import logging
logger = logging.getLogger(__name__)


class Executor(generic.Executor):
    def __init__(self, args, job, out, err):
        super(Executor, self).__init__(args, job, out, err)

    def untar_file(self, lfn, job):
        pfn = os.path.join(job.workdir, lfn)
        command = "tar -xf %s -C %s" % (pfn, job.workdir)
        logger.info("Untar file: %s" % command)
        status, output = commands.getstatusoutput(command)
        logger.info("status: %s, output: %s\n" % (status, output))

    def before_payload(self, job):
        """
        Functions to run before payload

        :param job: job object
        """
        log = get_logger(job.jobid, logger)

        log.info("untar input tar files for eventservicemerge job")
        for fspec in job.indata:
            if fspec.is_tar:
                self.untar_file(fspec.lfn, job)

        log.info("Processing writeToFile for eventservicemerge job")
        job.process_writetofile()

        super(Executor, self).before_payload(job)
