#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import os

from pilot.util.constants import PILOT_KILL_SIGNAL, LOG_TRANSFER_NOT_DONE
from pilot.util.timing import get_time_since

import logging
logger = logging.getLogger(__name__)


def should_abort(args, limit=30, label=''):
    """
    Abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set).

    :param args: pilot arguments object.
    :param limit: optional time limit (int).
    :param label: optional label prepending log messages (string).
    :return: True if graceful_stop has been set (and less than optional time limit has passed since maxtime) or False
    """

    abort = False
    if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility reasons
        if os.environ.get('REACHED_MAXTIME', None) and limit:
            time_since = get_time_since(0, PILOT_KILL_SIGNAL, args)
            if time_since < limit:
                logger.warning('%s:received graceful stop - %d s ago, continue for now' % (label, time_since))
            else:
                abort = True
        else:
            logger.warning('%s:received graceful stop - abort after this iteration' % label)
            abort = True

    return abort


def update_job_status(args, pandaid, key, value):
    """

    :param args: pilot arguments object.
    :param pandaid: PanDA job id (string).
    :param key: key name (string).
    :param value: key value (string).
    :return:
    """

    if pandaid not in args.job_status:
        args.job_status[pandaid] = {}
    args.job_status[pandaid][key] = value


def get_job_status(args, pandaid, key):
    """
    This function will return the key value for the given pandaid.
    E.g. get_job_status(12345678, 'LOG_TRANSFER') -> 'IN_PROGRESS'
    job_status = { pandaid: {key1: value1, ..}, ..}

    :param args: pilot arguments object.
    :param pandaid: PanDA job id (string).
    :param key: key name (string).
    :return: key value (string).
    """

    value = None
    dic = args.job_status.get(pandaid, None)
    if dic:
        value = dic.get(key, None)

    return value


def get_log_transfer(args, job, key='LOG_TRANSFER'):
    """

    Return the current log transfer status.
    LOG_TRANSFER_NOT_DONE is returned if job object is not defined.

    :param args: pilot args object.
    :param job: job object.
    :param key: optional key name, should be 'LOG_TRANSFER'
    :return: key value (string).
    """

    if job:
        log_transfer = get_job_status(args, job.jobid, key)
    else:
        log_transfer = LOG_TRANSFER_NOT_DONE

    return log_transfer
