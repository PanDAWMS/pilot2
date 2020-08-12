#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020
# - Tadashi Maeno, tadashi.maeno@cern.ch, 2020

import os

from pilot.util.auxiliary import get_logger
from pilot.util.config import config
from pilot.util.filehandling import read_file

import logging
logger = logging.getLogger(__name__)


def interpret(job):
    """
    Interpret the payload, look for specific errors in the stdout.

    :param job: job object
    :return: exit code (payload) (int).
    """

    log = get_logger(job.jobid)
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    message = 'payload stdout dump\n'
    message += read_file(stdout)
    log.debug(message)
    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
    message = 'payload stderr dump\n'
    message += read_file(stderr)
    log.debug(message)

    return 0
