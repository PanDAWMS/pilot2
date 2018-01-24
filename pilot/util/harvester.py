#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from os import environ, remove
from os.path import exists, join

from pilot.util.filehandling import write_json
from pilot.util.config import config
from pilot.common.exception import FileHandlingFailure

import logging
logger = logging.getLogger(__name__)


def get_job_request_file_name():
    """
    Return the name of the job request file as defined in the pilot config file.

    :return: job request file name.
    """

    return join(environ['PILOT_HOME'], config.Harvester.job_request_file)


def remove_job_request_file():
    """
    Remove an old job request file when it is no longer needed.

    :return:
    """

    path = get_job_request_file_name()
    try:
        remove(path)
    except IOError as e:
        logger.warning('failed to remove %s: %s' % (path, e))
    else:
        logger.info('removed %s' % path)


def request_new_jobs(nJobs=1):
    """
    Inform Harvester that the pilot is ready to process new jobs by creating a job request file with the desired
    number of jobs.

    :param nJobs: Number of jobs. Default is 1 since on grids and clouds the pilot does not know how many jobs it can
    process before it runs out of time.
    :return:
    """

    path = get_job_request_file_name()
    dictionary = { 'nJobs': nJobs }

    # write it to file
    try:
        write_json(path, dictionary)
    except FileHandlingFailure:
        raise FileHandlingFailure
