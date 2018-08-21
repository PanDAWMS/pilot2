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

from pilot.common.errorcodes import ErrorCodes
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


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


def shell_exit_code(exit_code):
    """
    Translate the pilot exit code to a proper exit code for the shell (wrapper).

    :param exit_code: pilot error code (int).
    :return: standard shell exit code (int).
    """

    # Error code translation dictionary
    # FORMAT: { pilot_error_code : [ shell_error_code, meaning ], .. }

    # Restricting user (pilot) exit codes to the range 64 - 113, as suggested by http://tldp.org/LDP/abs/html/exitcodes.html
    # Using exit code 137 for kill signal error codes (this actually means a hard kill signal 9, (128+9), 128+2 would mean CTRL+C)

    error_code_translation_dictionary = {
        -1: [64, "Site offline"],
        errors.GENERALERROR: [65, "General pilot error, consult batch log"],
        errors.MKDIR: [66, "Could not create directory"],
        errors.NOSUCHFILE: [67, "No such file or directory"],
        errors.NOVOMSPROXY: [68, "Voms proxy not valid"],
        errors.NOLOCALSPACE: [69, "No space left on local disk"],
        # errors.PILOTEXC: [70, "Exception caught by pilot"],
        # errors.QUEUEDATA: [71, "Pilot could not download queuedata"],
        # errors.QUEUEDATANOTOK: [72, "Pilot found non-valid queuedata"],
        # errors.NOSOFTWAREDIR: [73, "Software directory does not exist"],
        errors.KILLSIGNAL: [137, "General kill signal"], # Job terminated by unknown kill signal
        errors.SIGTERM: [143, "Job killed by signal: SIGTERM"], # 128+15
        errors.SIGQUIT: [131, "Job killed by signal: SIGQUIT"], # 128+3
        errors.SIGSEGV: [139, "Job killed by signal: SIGSEGV"], # 128+11
        errors.SIGXCPU: [158, "Job killed by signal: SIGXCPU"], # 128+30
        errors.SIGUSR1: [144, "Job killed by signal: SIGUSR1"], # 128+16
        errors.SIGBUS: [138, "Job killed by signal: SIGBUS"]   # 128+10
    }

    if exit_code in error_code_translation_dictionary:
        return error_code_translation_dictionary.get(exit_code)[0]  # Only return the shell exit code, not the error meaning
    elif exit_code != 0:
        logger.warning("no translation to shell exit code for error code %d" % (exit_code))
        return 1
    else:
        return 0


def declare_failed_by_kill(job, queue, sig):
    """
    Declare the job failed by a kill signal and put it in a suitable failed queue.
    E.g. queue=queues.failed_data_in, if the kill signal was received during stage-in.

    :param job: job object.
    :param queue: queue object.
    :param sig: signal.
    :return:
    """

    job.state = 'failed'
    error_code = errors.get_kill_signal_error_code(sig)
    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error_code)
    queue.put(job)
