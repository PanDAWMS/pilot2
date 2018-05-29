#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import logging
logger = logging.getLogger(__name__)


def read_pilot_timing():
    pass


def write_pilot_timing():
    pass


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
    Time measurement are time.time() values.

    :param job_id: PanDA job id (string).
    :param timing_constant_1:
    :param timing_constant_2:
    :return: time difference in seconds (int).
    """

    diff = 0

    try:
        log = logger.getChild(job_id)
    except Exception:
        log = logger

    # first read the current pilot timing dictionary
    timing_dictionary = read_pilot_timing()

    if job_id in timing_dictionary:
        # extract time measurements
        time_measurement_dictionary = timing_dictionary.get(job_id, None)
        if time_measurement_dictionary:
            time_measurement_1 = time_measurement_dictionary.get(timing_constant_1, None)
            if not time_measurement_1:
                log.warning('failed to extract time measurement %d from %s' %
                            (timing_constant_1, time_measurement_dictionary))
            time_measurement_2 = time_measurement_dictionary.get(timing_constant_2, None)
            if not time_measurement_2:
                log.warning('failed to extract time measurement %d from %s' %
                            (timing_constant_2, time_measurement_dictionary))
            if time_measurement_1 and time_measurement_2:
                diff = time_measurement_2 - time_measurement_1
        else
            log.warning('failed to extract time measurement dictionary from %s' % str(timing_dictionary))
    else:
        log.warning('job id %s not found in timing dictionary' % job_id)

    # always return a positive number
    if diff < 0:
        diff = -diff

    return diff
