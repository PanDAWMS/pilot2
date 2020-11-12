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

from pilot.util.config import config
from pilot.util.filehandling import read_file, tail

import logging
logger = logging.getLogger(__name__)


def interpret(job):
    """
    Interpret the payload, look for specific errors in the stdout.

    :param job: job object
    :return: exit code (payload) (int).
    """

    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    message = 'payload stdout dump\n'
    message += read_file(stdout)
    logger.debug(message)
    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
    message = 'payload stderr dump\n'
    message += read_file(stderr)
    logger.debug(message)

    return 0


def get_log_extracts(job, state):
    """
    Extract special warnings and other other info from special logs.
    This function also discovers if the payload had any outbound connections.

    :param job: job object.
    :param state: job state (string).
    :return: log extracts (string).
    """

    logger.info("building log extracts (sent to the server as \'pilotLog\')")

    # for failed/holding jobs, add extracts from the pilot log file, but always add it to the pilot log itself
    extracts = ""
    _extracts = get_pilot_log_extracts(job)
    if _extracts != "":
        logger.warning('detected the following tail of warning/fatal messages in the pilot log:\n%s' % _extracts)
        if state == 'failed' or state == 'holding':
            extracts += _extracts

    return extracts


def get_pilot_log_extracts(job):
    """
    Get the extracts from the pilot log (warning/fatal messages, as well as tail of the log itself).

    :param job: job object.
    :return: tail of pilot log (string).
    """

    extracts = ""

    path = os.path.join(job.workdir, config.Pilot.pilotlog)
    if os.path.exists(path):
        # get the last 20 lines of the pilot log in case it contains relevant error information
        _tail = tail(path, nlines=20)
        if _tail != "":
            if extracts != "":
                extracts += "\n"
            extracts += "- Log from %s -\n" % config.Pilot.pilotlog
            extracts += _tail
    else:
        logger.warning('pilot log file does not exist: %s' % path)

    return extracts
