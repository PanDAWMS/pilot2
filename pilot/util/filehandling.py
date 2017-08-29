#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import os
import time

#import logging
#logger = logging.getLogger(__name__)


def get_pilot_work_dir(workdir):
    """
    Return the full path to the main PanDA Pilot work directory. Called once at the beginning of the batch job.

    :param workdir: The full path to where the main work directory should be created
    :return: The name of main work directory
    """

    jobworkdir = "PanDA_Pilot2_%d_%s" % (os.getpid(), str(int(time.time())))
    return os.path.join(workdir, jobworkdir)


def create_pilot_work_dir(workdir):
    """
    Create the main PanDA Pilot work directory.
    :param workdir: Full path to the directory to be created
    :return:
    """

    try:
        os.makedirs(workdir)
        os.chmod(workdir, 0770)
    except Exception, e:
        pass
 #       logger.error('could not create main work directory: %s' % e)
        # throw PilotException here
