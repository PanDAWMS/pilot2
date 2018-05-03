#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from pilot.util.container import execute
from pilot.util.auxiliary import time_stamp, whoami
from pilot.util.processes import kill_processes
from pilot.util.filehandling import remove_files
from pilot.util.config import config
from pilot.common.errorcodes import ErrorCodes

import os
import time
import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def looping_job(job, mt):
    """
    Looping job detection algorithm.
    Identify hanging tasks/processes. Did the stage-in/out finish within allowed time limit, or did the payload update
    any files recently? The files must have been touched within the given looping_limit, or the process will be
    terminated.

    :param job: job object.
    :param mt: `MonitoringTime` object.
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    log = logger.getChild(job.jobid)
    log.info('checking for looping job')

    looping_limit = get_looping_job_limit(job.is_analysis())

    if job.state == 'stagein':
        # set job.state to stagein during stage-in before implementing this algorithm
        pass
    elif job.state == 'stageout':
        # set job.state to stageout during stage-out before implementing this algorithm
        pass
    else:
        # most likely in the 'running' state, but use the catch-all 'else'

        # get the time when the files in the workdir were last touched. in case no file was touched since the last
        # check, the returned value will be the same as the previous time
        time_last_touched = get_time_for_last_touch(job, mt, looping_limit)

        # the payload process is considered to be looping if it's files have not been touched within looping_limit time
        if time_last_touched:
            ct = int(time.time())
            log.info('current time: %d' % ct)
            log.info('last time files were touched: %s' % mt.ct_looping_last_touched)
            log.info('looping limit: %d s' % looping_limit)
            if ct - time_last_touched > looping_limit:
                kill_looping_job(job)
        else:
            log.info('no files were touched yet')

    return exit_code, diagnostics


def get_time_for_last_touch(job, mt, looping_limit):
    """
    Return the time when the files in the workdir were last touched.
    in case no file was touched since the last check, the returned value will be the same as the previous time.

    :param job: job object.
    :param mt: `MonitoringTime` object.
    :param looping_limit: looping limit in seconds.
    :return: time in seconds since epoch (int).
    """

    log = logger.getChild(job.jobid)

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    loopingjob_definitions = __import__('pilot.user.%s.loopingjob_definitions' % pilot_user,
                                        globals(), locals(), [pilot_user], -1)

    # locate all files that were modified the last N minutes
    cmd = "find %s -mmin -%d" % (job.workdir, int(looping_limit / 60))
    exit_code, stdout, stderr = execute(cmd)
    if exit_code == 0:
        if stdout != "":
            files = stdout.split("\n")  # find will always add an \n even for single entries

            # remove unwanted list items (*.py, *.pyc, workdir, ...)
            files = loopingjob_definitions.remove_unwanted_files(job.workdir, files)
            if files != []:
                log.info("found %d files that were recently updated (e.g. file %s)" % (len(files), files[0]))
                # update the current system time
                mt.update('ct_looping_last_touched')
            else:
                log.warning("found no recently updated files!")
        else:
            log.warning('found no recently updated files')
    else:
        log.warning('find command failed: %d, %s, %s' % (exit_code, stdout, stderr))

    return mt.ct_looping_last_touched


def kill_looping_job(job):
    """
    Kill the looping process.
    TODO: add allow_looping_job() exp. spec?

    :param job: job object.
    :return: (updated job object.)
    """

    log = logger.getChild(job.jobid)

    # the child process is looping, kill it
    diagnostics = "pilot has decided to kill looping job %s at %s" % (job.jobid, time_stamp())
    log.fatal(diagnostics)

    cmd = 'ps -fwu %s' % whoami()
    exit_code, stdout, stderr = execute(cmd, mute=True)
    log.info("%s: %s" % (cmd + '\n', stdout))

    cmd = 'ls -ltr %s' % (job.workdir)
    exit_code, stdout, stderr = execute(cmd, mute=True)
    log.info("%s: %s" % (cmd + '\n', stdout))

    cmd = 'ps -o pid,ppid,sid,pgid,tpgid,stat,comm -u %s' % whoami()
    exit_code, stdout, stderr = execute(cmd, mute=True)
    log.info("%s: %s" % (cmd + '\n', stdout))

    kill_processes(job.pid, job.pgrp)

    # set the relevant error code
    if job.state == 'stagein':
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINTIMEOUT)
    elif job.state == 'stageout':
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTTIMEOUT)
    else:
        # most likely in the 'running' state, but use the catch-all 'else'
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.LOOPINGJOB)
    job.state = 'failed'

    # remove any lingering input files from the work dir
    if job.infiles:
        if len(job.infiles) > 0:
            _ = remove_files(job.workdir, job.infiles)


def get_looping_job_limit(is_analysis):
    """
    Get the time limit for looping job detection.

    :param is_analysis: Boolean, True if user analysis job, False otherwise.
    :return: looping job time limit (int).
    """

    try:
        looping_limit = int(config.Pilot.looping_limit_default_prod)
        if is_analysis:
            looping_limit = int(config.Pilot.looping_limit_default_user)
    except ValueError as e:
        looping_limit = 12 * 3600
        logger.warning('exception caught: %s (using default looping limit: %d s)' % (e, looping_limit))

    return looping_limit
