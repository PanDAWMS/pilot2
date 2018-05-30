#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# Note: The Pilot 2 modules that need to record timing measurements, can do so using the add_to_pilot_timing() function.
# When the timing measurements need to be recorded, the high-level functions, e.g. get_getjob_time(), can be used.

import os

from pilot.util.auxiliary import get_logger
from pilot.util.config import config
from pilot.util.constants import PILOT_T0, PILOT_PRE_GETJOB, PILOT_POST_GETJOB, PILOT_PRE_SETUP, PILOT_POST_SETUP, \
    PILOT_PRE_STAGEIN, PILOT_POST_STAGEIN, PILOT_PRE_PAYLOAD, PILOT_POST_PAYLOAD, PILOT_PRE_STAGEOUT, \
    PILOT_POST_STAGEOUT, PILOT_PRE_FINAL_UPDATE, PILOT_POST_FINAL_UPDATE, PILOT_END_TIME
from pilot.util.filehandling import read_json, write_json

import logging
logger = logging.getLogger(__name__)


def read_pilot_timing():
    """
    Read the pilot timing dictionary from file.

    :return: pilot timing dictionary (json dictionary).
    """

    pilot_timing_dictionary = {}

    path = os.path.join(os.environ.get('PILOT_WORK_DIR', ''), config.Pilot.timing_file)
    if os.path.exists(path):
        pilot_timing_dictionary = read_json(path)
    else:
        logger.warning('pilot timing dictionary does not exist: %s' % path)

    return pilot_timing_dictionary


def write_pilot_timing(job_id, pilot_timing_dictionary):
    """
    Write the given pilot timing dictionary to file.

    :param job_id: PanDA job id (string).
    :param pilot_timing_dictionary:
    :return:
    """

    log = get_logger(job_id)

    path = os.path.join(os.environ.get('PILOT_WORK_DIR', ''), config.Pilot.timing_file)
    if write_json(path, pilot_timing_dictionary):
        log.info('updated pilot timing dictionary: %s' % (path))
    else:
        log.info('failed to update pilot timing dictionary: %s' % (path))


def add_to_pilot_timing(job_id, timing_constant, time_measurement):
    """
    Add the given timing contant and measurement got job_id to the pilot timing dictionary.

    :param job_id: PanDA job id (string).
    :param timing_constant: timing constant (string).
    :param time_measurement: time measurement (float).
    :return:
    """

    # get the timing dictionary from file
    pilot_timing_dictionary = read_pilot_timing()

    if pilot_timing_dictionary == {}:
        pilot_timing_dictionary[job_id] = {timing_constant: time_measurement}
    else:
        if job_id not in pilot_timing_dictionary:
            pilot_timing_dictionary[job_id] = {}
        pilot_timing_dictionary[job_id][timing_constant] = time_measurement

    # update the file
    write_pilot_timing(job_id, pilot_timing_dictionary)


def get_getjob_time(job_id):
    """
    High level function that returns the time for the getjob operation for the given job_id.

    :param job_id: PanDA job id (string).
    :return: time in seconds (int).
    """

    return get_time_difference(job_id, PILOT_PRE_GETJOB, PILOT_POST_GETJOB)


def get_setup_time(job_id):
    """
    High level function that returns the time for the setup operation for the given job_id.

    :param job_id: PanDA job id (string).
    :return: time in seconds (int).
    """

    return get_time_difference(job_id, PILOT_PRE_SETUP, PILOT_POST_SETUP)


def get_stagein_time(job_id):
    """
    High level function that returns the time for the stage-in operation for the given job_id.

    :param job_id: PanDA job id (string).
    :return: time in seconds (int).
    """

    return get_time_difference(job_id, PILOT_PRE_STAGEIN, PILOT_POST_STAGEIN)


def get_stageout_time(job_id):
    """
    High level function that returns the time for the stage-out operation for the given job_id.

    :param job_id: PanDA job id (string).
    :return: time in seconds (int).
    """

    return get_time_difference(job_id, PILOT_PRE_STAGEOUT, PILOT_POST_STAGEOUT)


def get_payload_execution_time(job_id):
    """
    High level function that returns the time for the payload execution for the given job_id.

    :param job_id: PanDA job id (string).
    :return: time in seconds (int).
    """

    return get_time_difference(job_id, PILOT_PRE_PAYLOAD, PILOT_POST_PAYLOAD)


def get_final_update_time(job_id):
    """
    High level function that returns the time for execution the final update for the given job_id.

    :param job_id: PanDA job id (string).
    :return: time in seconds (int).
    """

    return get_time_difference(job_id, PILOT_PRE_FINAL_UPDATE, PILOT_POST_FINAL_UPDATE)


def get_total_pilot_time(job_id):
    """
    High level function that returns the end time for the given job_id.
    This means the wall time that has passed from the start of the pilot until after the last job update.

    :param job_id: PanDA job id (string).
    :return: time in seconds (int).
    """

    return get_time_difference(job_id, PILOT_T0, PILOT_END_TIME)


def get_time_difference(job_id, timing_constant_1, timing_constant_2):
    """
    Return the positive time difference between the given constants.
    The order is not important and a positive difference is always returned. The function collects the time measurements
    corresponding to the given timing constants from the pilot timing file.
    The job_id is used internally as a dictionary key. The given timing constants and their timing measurements, belong
    to the given job_id.
    Structure of pilot timing dictionary:
        { job_id: { <timing_constant_1>: <time measurement in seconds since epoch>, .. }
    job_id = 0 means timing information from wrapper. Timing constants are defined in pilot.util.constants.
    Time measurement are time.time() values. The float value will be converted to an int as a last step.

    :param job_id: PanDA job id (string).
    :param timing_constant_1:
    :param timing_constant_2:
    :return: time difference in seconds (int).
    """

    diff = 0

    log = get_logger(job_id)

    # first read the current pilot timing dictionary
    timing_dictionary = read_pilot_timing()

    if job_id in timing_dictionary:
        # extract time measurements
        time_measurement_dictionary = timing_dictionary.get(job_id, None)
        if time_measurement_dictionary:
            time_measurement_1 = time_measurement_dictionary.get(timing_constant_1, None)
            if not time_measurement_1:
                log.warning('failed to extract time measurement %d from %s (no such key)' %
                            (timing_constant_1, time_measurement_dictionary))
            time_measurement_2 = time_measurement_dictionary.get(timing_constant_2, None)
            if not time_measurement_2:
                log.warning('failed to extract time measurement %d from %s (no such key)' %
                            (timing_constant_2, time_measurement_dictionary))
            if time_measurement_1 and time_measurement_2:
                diff = time_measurement_2 - time_measurement_1
        else:
            log.warning('failed to extract time measurement dictionary from %s' % str(timing_dictionary))
    else:
        log.warning('job id %s not found in timing dictionary' % job_id)

    # always return a positive number
    if diff < 0:
        diff = -diff

    # convert to int as a last step
    try:
        diff = int(diff)
    except Exception as e:
        log.warning('failed to convert %s to int: %s (will reset to 0)' % (diff, e))
        diff = 0

    return diff
