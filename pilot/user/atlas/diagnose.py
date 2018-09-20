#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import json
import os

from pilot.util.auxiliary import get_logger
from pilot.util.config import config
from pilot.util.filehandling import get_guid

from .common import update_job_data


def interpret(job, code):
    """
    Interpret the payload, look for specific errors in the stdout.

    :param job: job object
    :param code: payload execution exit code (Int).
    :return: exit code (updated from payload) (Int).
    """
    exit_code = code

    # extract errors from job report
    process_job_report(job)

    return exit_code


def process_job_report(job):
    """
    Process the job report produced by the payload/transform if it exists.
    Payload error codes and diagnostics, as well as payload metadata (for output files) and stageout type will be
    extracted. The stageout type is either "all" (i.e. stage-out both output and log files) or "log" (i.e. only log file
    will be staged out).
    Note: some fields might be experiment specific. A call to a user function is therefore also done.

    :param job: job dictionary will be updated by the function and several fields set.
    :return:
    """

    log = get_logger(job.jobid)
    path = os.path.join(job.workdir, config.Payload.jobreport)
    if not os.path.exists(path):
        log.warning('job report does not exist: %s (any missing output file guids must be generated)' % path)

        # add missing guids
        for dat in job.outdata:
            if not dat.guid:
                dat.guid = get_guid()
                log.warning('guid not set: generated guid=%s for lfn=%s' % (dat.guid, dat.lfn))

    else:
        with open(path) as data_file:
            # compulsory field; the payload must produce a job report (see config file for file name), attach it to the
            # job object
            job.metadata = json.load(data_file)

            #
            update_job_data(job)

            # compulsory fields
            try:
                job.exitcode = job.metadata['exitCode']
            except Exception as e:
                log.warning('could not find compulsory payload exitCode in job report: %s (will be set to 0)' % e)
                job.exitcode = 0
            else:
                log.info('extracted exit code from job report: %d' % job.exitcode)
            try:
                job.exitmsg = job.metadata['exitMsg']
            except Exception as e:
                log.warning('could not find compulsory payload exitMsg in job report: %s (will be set to empty string)' % e)
                job.exitmsg = ""
            else:
                log.info('extracted exit message from job report: %s' % job.exitmsg)
