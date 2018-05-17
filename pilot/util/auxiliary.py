#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os
import time

from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def time_stamp():
    """
    Return ISO-8601 compliant date/time format

    :return: time information
    """

    tmptz = time.timezone
    if tmptz > 0:
        signstr = '-'
    else:
        signstr = '+'
    tmptz_hours = int(tmptz / 3600)

    return str("%s%s%02d%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), signstr, tmptz_hours,
                                 int(tmptz / 60 - tmptz_hours * 60)))


def set_time_consumed(t_tuple):
    """
    Set the system+user time spent by the payload.
    The cpuConsumptionTime is the system+user time while wall time is encoded in pilotTiming (third number).
    Previously the cpuConsumptionTime was "corrected" with a scaling factor but this was deemed outdated and is now set
    to 1.
    The t_tuple is defined as map(lambda x, y:x-y, t1, t0), here t0 and t1 are os.times() measured before and after
    the payload execution command.

    :param t_tuple: map(lambda x, y:x-y, t1, t0)
    :return: cpu_consumption_unit, cpu_consumption_time, cpu_conversion_factor
    """

    t_tot = reduce(lambda x, y: x + y, t_tuple[2:3])
    cpu_conversion_factor = 1.0
    cpu_consumption_unit = "s"  # used to be "kSI2kseconds"
    cpu_consumption_time = int(t_tot * cpu_conversion_factor)

    return cpu_consumption_unit, cpu_consumption_time, cpu_conversion_factor


def get_batchsystem_jobid():
    """
    Identify and return the batch system job id (will be reported to the server)

    :return: batch system job id
    """

    # BQS (e.g. LYON)
    batchsystem_dict = {'QSUB_REQNAME': 'BQS',
                        'BQSCLUSTER': 'BQS',  # BQS alternative
                        'PBS_JOBID': 'Torque',
                        'LSB_JOBID': 'LSF',
                        'JOB_ID': 'Grid Engine',  # Sun's Grid Engine
                        'clusterid': 'Condor',  # Condor (variable sent through job submit file)
                        'SLURM_JOB_ID': 'SLURM'}

    for key, value in batchsystem_dict.iteritems():
        if key in os.environ:
            return value, key

    # Condor (get jobid from classad file)
    if '_CONDOR_JOB_AD' in os.environ:
        from commands import getoutput
        return "Condor", getoutput(
            'sed -n "s/GlobalJobId.*\\"\\(.*\\)\\".*/\\1/p" %s' % os.environ.get("_CONDOR_JOB_AD"))

    return None, ""


def get_job_scheduler_id():
    """
    Get the job scheduler id from the environment variable PANDA_JSID

    :return: job scheduler id (string)
    """
    return os.environ.get("PANDA_JSID", "unknown")


def get_pilot_id():
    """
    Get the pilot id from the environment variable GTAG

    :return: pilot id (string)
    """

    return os.environ.get("GTAG", "unknown")


def whoami():
    """
    Return the name of the pilot user.

    :return: whoami output (string).
    """

    exit_code, who_am_i, stderr = execute('whoami', mute=True)

    return who_am_i
