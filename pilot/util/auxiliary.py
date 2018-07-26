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
    sign_str = '+'
    if tmptz > 0:
        sign_str = '-'
    tmptz_hours = int(tmptz / 3600)

    return str("%s%s%02d:%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), sign_str, abs(tmptz_hours),
                                  int(tmptz / 60 - tmptz_hours * 60)))


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


def get_logger(job_id):
    """
    Return the logger object.
    Use this function to get the proper logger object. It relies on a pythno 2.7 function, getChild(), but if the queue
    is only using Python 2.6, the standard logger object will be returned instead.

    :param jod_id: PanDA job id (string).
    :return: logger object.
    """

    try:
        log = logger.getChild(job_id)
    except Exception:
        log = logger
    return log
