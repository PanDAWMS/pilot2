#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2020

from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import whoami, set_pilot_state, cut_output
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import remove_files, find_latest_modified_file, verify_file_list
from pilot.util.parameters import convert_to_int
from pilot.util.processes import kill_processes
from pilot.util.timing import time_stamp

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

    logger.info('checking for looping job')

    looping_limit = get_looping_job_limit(job)

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
            logger.info('current time: %d' % ct)
            logger.info('last time files were touched: %d' % time_last_touched)
            logger.info('looping limit: %d s' % looping_limit)
            if ct - time_last_touched > looping_limit:
                try:
                    kill_looping_job(job)
                except Exception as e:
                    logger.warning('exception caught: %s' % e)
        else:
            logger.info('no files were touched')

    return exit_code, diagnostics


def get_time_for_last_touch(job, mt, looping_limit):
    """
    Return the time when the files in the workdir were last touched.
    in case no file was touched since the last check, the returned value will be the same as the previous time.

    :param job: job object.
    :param mt: `MonitoringTime` object.
    :param looping_limit: looping limit in seconds.
    :return: time in seconds since epoch (int) (or None in case of failure).
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    loopingjob_definitions = __import__('pilot.user.%s.loopingjob_definitions' % pilot_user,
                                        globals(), locals(), [pilot_user], 0)  # Python 2/3

    # locate all files that were modified the last N minutes
    cmd = "find %s -mmin -%d" % (job.workdir, int(looping_limit / 60))
    exit_code, stdout, stderr = execute(cmd)
    if exit_code == 0:
        if stdout != "":
            files = stdout.split("\n")  # find might add a \n even for single entries

            # remove unwanted list items (*.py, *.pyc, workdir, ...)
            files = loopingjob_definitions.remove_unwanted_files(job.workdir, files)
            if files:
                logger.info('found %d files that were recently updated' % len(files))

                updated_files = verify_file_list(files)

                # now get the mod times for these file, and identify the most recently update file
                latest_modified_file, mtime = find_latest_modified_file(updated_files)
                if latest_modified_file:
                    logger.info("file %s is the most recently updated file (at time=%d)" % (latest_modified_file, mtime))
                else:
                    logger.warning('looping job algorithm failed to identify latest updated file')
                    return mt.ct_looping_last_touched

                # store the time of the last file modification
                mt.update('ct_looping_last_touched', modtime=mtime)
            else:
                logger.warning("found no recently updated files!")
        else:
            logger.warning('found no recently updated files')
    else:
        # cut the output if too long
        stdout = cut_output(stdout)
        stderr = cut_output(stderr)
        logger.warning('find command failed: %d, %s, %s' % (exit_code, stdout, stderr))

    return mt.ct_looping_last_touched


def kill_looping_job(job):
    """
    Kill the looping process.
    TODO: add allow_looping_job() exp. spec?

    :param job: job object.
    :return: (updated job object.)
    """

    # the child process is looping, kill it
    diagnostics = "pilot has decided to kill looping job %s at %s" % (job.jobid, time_stamp())
    logger.fatal(diagnostics)

    cmd = 'ps -fwu %s' % whoami()
    exit_code, stdout, stderr = execute(cmd, mute=True)
    logger.info("%s: %s" % (cmd + '\n', stdout))

    cmd = 'ls -ltr %s' % (job.workdir)
    exit_code, stdout, stderr = execute(cmd, mute=True)
    logger.info("%s: %s" % (cmd + '\n', stdout))

    cmd = 'ps -o pid,ppid,sid,pgid,tpgid,stat,comm -u %s' % whoami()
    exit_code, stdout, stderr = execute(cmd, mute=True)
    logger.info("%s: %s" % (cmd + '\n', stdout))

    cmd = 'pstree -g -a'
    exit_code, stdout, stderr = execute(cmd, mute=True)
    logger.info("%s: %s" % (cmd + '\n', stdout))

    # set the relevant error code
    if job.state == 'stagein':
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINTIMEOUT)
    elif job.state == 'stageout':
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTTIMEOUT)
    else:
        # most likely in the 'running' state, but use the catch-all 'else'
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.LOOPINGJOB)
    set_pilot_state(job=job, state="failed")

    # remove any lingering input files from the work dir
    lfns, guids = job.get_lfns_and_guids()
    if lfns:
        ec = remove_files(job.workdir, lfns)
        if ec != 0:
            logger.warning('failed to remove all files')

    kill_processes(job.pid)


def get_looping_job_limit(job):
    """
    Get the time limit for looping job detection.

    :param job: job object.
    :return: looping job time limit (int).
    """

    is_analysis = job.is_analysis()
    looping_limit = convert_to_int(config.Pilot.looping_limit_default_prod, default=12 * 3600)
    if is_analysis:
        looping_limit = convert_to_int(config.Pilot.looping_limit_default_user, default=3 * 3600)

    if job.maxcpucount and job.maxcpucount >= config.Pilot.looping_limit_min_default:
        _looping_limit = max(config.Pilot.looping_limit_min_default, job.maxcpucount)
    else:
        _looping_limit = max(looping_limit, job.maxcpucount)

    if _looping_limit != looping_limit:
        logger.info("task request has updated looping job limit from %d s to %d s using maxCpuCount" % (looping_limit, _looping_limit))
        looping_limit = _looping_limit
    else:
        logger.info("using standard looping job limit: %d s" % looping_limit)

    return looping_limit
