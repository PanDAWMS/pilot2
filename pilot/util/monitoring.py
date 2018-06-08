#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# This module contains implementations of job monitoring tasks

import os
import time
from subprocess import PIPE

from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import get_logger
from pilot.util.config import config, human2bytes
from pilot.util.container import execute
from pilot.util.filehandling import get_directory_size, remove_files
from pilot.util.loopingjob import looping_job
from pilot.util.parameters import convert_to_int
from pilot.util.processes import get_instant_cpu_consumption_time
from pilot.util.workernode import get_local_disk_space

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def job_monitor_tasks(job, mt, verify_proxy):
    """
    Perform the tasks for the job monitoring.

    :param job: job object.
    :param mt: `MonitoringTime` object.
    :param verify_proxy: True if the proxy should be verified. False otherwise.
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    log = get_logger(job.jobid)
    current_time = int(time.time())

    # update timing info for running jobs (to avoid an update after the job has finished)
    if job.state == 'running':
        cpuconsumptiontime = get_instant_cpu_consumption_time(job.pid)
        job.cpuconsumptiontime = int(cpuconsumptiontime)
        job.cpuconsumptionunit = "s"
        job.cpuconversionfactor = 1.0
        log.info('CPU consumption time for pid=%d: %f (rounded to %d)' %
                 (job.pid, cpuconsumptiontime, job.cpuconsumptiontime))

    # should the proxy be verified?
    if verify_proxy:
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        userproxy = __import__('pilot.user.%s.proxy' % pilot_user, globals(), locals(), [pilot_user], -1)

        # is it time to verify the proxy?
        proxy_verification_time = convert_to_int(config.Pilot.proxy_verification_time)
        if current_time - mt.get('ct_proxy') > proxy_verification_time:
            # is the proxy still valid?
            exit_code, diagnostics = userproxy.verify_proxy()
            if exit_code != 0:
                return exit_code, diagnostics
            else:
                # update the ct_proxy with the current time
                mt.update('ct_proxy')

    # is it time to check for looping jobs?
    log.info('current_time - mt.get(ct_looping) = %d' % (current_time - mt.get('ct_looping')))
    log.info('config.Pilot.looping_verifiction_time = %s' % config.Pilot.looping_verifiction_time)
    looping_verifiction_time = convert_to_int(config.Pilot.looping_verifiction_time)
    if current_time - mt.get('ct_looping') > looping_verifiction_time:
        # is the job looping?
        try:
            exit_code, diagnostics = looping_job(job, mt)
        except Exception as e:
            exit_code = errors.UNKNOWNEXCEPTION
            diagnostics = 'exception caught in looping job algorithm: %s' % e
            log.warning(diagnostics)
            return exit_code, diagnostics
        else:
            if exit_code != 0:
                return exit_code, diagnostics

        # update the ct_proxy with the current time
        mt.update('ct_looping')

    # is the job using too much space?
    disk_space_verification_time = convert_to_int(config.Pilot.disk_space_verification_time)
    if current_time - mt.get('ct_diskspace') > disk_space_verification_time:
        # time to check the disk space

        # check the size of the payload stdout - __checkPayloadStdout in Monitor
        # check_payload_stdout()

        # check the local space, if it's enough left to keep running the job
        exit_code, diagnostics = check_local_space()
        if exit_code != 0:
            return exit_code, diagnostics

        # check the size of the workdir
        exit_code, diagnostics = check_work_dir(job)
        if exit_code != 0:
            return exit_code, diagnostics

        # update the ct_diskspace with the current time
        mt.update('ct_diskspace')

    # Is the payload stdout within allowed limits?

    # Are the output files within allowed limits?

    # make sure that any utility commands are still running
    if job.utilities != {}:
        job = utility_monitor(job)

    # send heartbeat

    return exit_code, diagnostics


def utility_monitor(job):
    """
    Make sure that any utility commands are still running.
    In case a utility tool has crashed, this function may restart the process.
    The function is used by the job monitor thread.

    :param job: job object.
    :return: updated job object.
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    usercommon = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], -1)

    log = get_logger(job.jobid)

    # loop over all utilities
    for utcmd in job.utilities.keys():

        # make sure the subprocess is still running
        utproc = job.utilities[utcmd][0]
        if not utproc.poll() is None:
            # if poll() returns anything but None it means that the subprocess has ended - which it
            # should not have done by itself
            utility_subprocess_launches = job.utilities[utcmd][1]
            if utility_subprocess_launches <= 5:
                log.warning('dectected crashed utility subprocess - will restart it')
                utility_command = job.utilities[utcmd][2]

                try:
                    proc1 = execute(utility_command, workdir=job.workdir, returnproc=True, usecontainer=True,
                                    stdout=PIPE, stderr=PIPE, cwd=job.workdir, queuedata=job.infosys.queuedata)
                except Exception as e:
                    log.error('could not execute: %s' % e)
                else:
                    # store process handle in job object, and keep track on how many times the
                    # command has been launched
                    job.utilities[utcmd] = [proc1, utility_subprocess_launches + 1, utility_command]
            else:
                log.warning('dectected crashed utility subprocess - too many restarts, will not restart %s again' %
                            utcmd)
        else:
            # log.info('utility %s is still running' % utcmd)

            # check the utility output (the selector option adds a substring to the output file name)
            filename = usercommon.get_utility_command_output_filename(utcmd, selector=True)
            path = os.path.join(job.workdir, filename)
            if os.path.exists(path):
                log.info('file: %s exists' % path)
            else:
                log.warning('file: %s does not exist' % path)
    return job


def check_local_space():
    """
    Do we have enough local disk space left to run the job?

    :return: pilot error code (0 if success, NOLOCALSPACE if failure)
    """

    ec = 0
    diagnostics = ""

    # is there enough local space to run a job?
    spaceleft = int(get_local_disk_space(os.getcwd())) * 1024 ** 2  # B (diskspace is in MB)
    free_space_limit = human2bytes(config.Pilot.free_space_limit)
    if spaceleft <= free_space_limit:
        diagnostics = 'too little space left on local disk to run job: %d B (need > %d B)' %\
                      (spaceleft, free_space_limit)
        ec = errors.NOLOCALSPACE
        logger.warning(diagnostics)
    else:
        logger.info('remaining disk space (%d B) is sufficient to download a job' % spaceleft)

    return ec, diagnostics


def check_work_dir(job):
    """
    Check the size of the work directory.
    The function also updates the workdirsizes list in the job object.

    :param job: job object.
    :return: exit code (int), error diagnostics (string)
    """

    exit_code = 0
    diagnostics = ""

    log = get_logger(job.jobid)

    if os.path.exists(job.workdir):
        # get the limit of the workdir
        maxwdirsize = get_max_allowed_work_dir_size(job.infosys.queuedata)

        if os.path.exists(job.workdir):
            workdirsize = get_directory_size(directory=job.workdir)

            # is user dir within allowed size limit?
            if workdirsize > maxwdirsize:

                diagnostics = "work directory (%s) is too large: %d B (must be < %d B)" % \
                              (job.workdir, workdirsize, maxwdirsize)
                log.fatal("%s" % diagnostics)

                cmd = 'ls -altrR %s' % job.workdir
                exit_code, stdout, stderr = execute(cmd, mute=True)
                log.info("%s: %s" % (cmd + '\n', stdout))

                # kill the job
                # pUtil.createLockFile(True, self.__env['jobDic'][k][1].workdir, lockfile="JOBWILLBEKILLED")
                kill_processes(job.pid, job.pgrp)
                job.state = 'failed'
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.USERDIRTOOLARGE)

                # remove any lingering input files from the work dir
                if job.infiles != []:
                    exit_code = remove_files(job.workdir, job.infiles)

                    # remeasure the size of the workdir at this point since the value is stored below
                    workdirsize = get_directory_size(directory=job.workdir)
            else:
                log.info("size of work directory %s: %d B (within %d B limit)" %
                         (job.workdir, workdirsize, maxwdirsize))

            # Store the measured disk space (the max value will later be sent with the job metrics)
            if workdirsize > 0:
                job.add_workdir_size(workdirsize)
        else:
            log.warning('job work dir does not exist: %s' % job.workdir)
    else:
        log.warning('skipping size check of workdir since it has not been created yet')

    return exit_code, diagnostics


def get_max_allowed_work_dir_size(queuedata):
    """
    Return the maximum allowed size of the work directory.

    :param queuedata: job.infosys.queuedata object.
    :return: max allowed work dir size in Bytes (int).
    """

    try:
        maxwdirsize = int(queuedata.maxwdir) * 1024 ** 2  # from MB to B, e.g. 16336 MB -> 17,129,537,536 B
    except Exception:
        max_input_size = get_max_input_size()
        maxwdirsize = max_input_size + config.Pilot.local_size_limit_stdout * 1024
        logger.info("work directory size check will use %d B as a max limit (maxinputsize [%d B] + local size limit for"
                    " stdout [%d B])" % (maxwdirsize, max_input_size, config.Pilot.local_size_limit_stdout * 1024))
    else:
        logger.info("work directory size check will use %d B as a max limit" % maxwdirsize)

    return maxwdirsize


def get_max_input_size(queuedata, megabyte=False):
    """
    Return a proper maxinputsize value.

    :param queuedata: job.infosys.queuedata object.
    :param megabyte: return results in MB (Boolean).
    :return: max input size (int).
    """

    _maxinputsize = queuedata.maxwdir  # normally 14336+2000 MB
    max_input_file_sizes = 14 * 1024 * 1024 * 1024  # 14 GB, 14336 MB (pilot default)
    max_input_file_sizes_mb = 14 * 1024  # 14336 MB (pilot default)
    if _maxinputsize != "":
        try:
            if megabyte:  # convert to MB int
                _maxinputsize = int(_maxinputsize)  # MB
            else:  # convert to B int
                _maxinputsize = int(_maxinputsize) * 1024 * 1024  # MB -> B
        except Exception as e:
            logger.warning("schedconfig.maxinputsize: %s" % e)
            if megabyte:
                _maxinputsize = max_input_file_sizes_mb
            else:
                _maxinputsize = max_input_file_sizes
    else:
        if megabyte:
            _maxinputsize = max_input_file_sizes_mb
        else:
            _maxinputsize = max_input_file_sizes

    if megabyte:
        logger.info("max input size = %d MB (pilot default)" % _maxinputsize)
    else:
        logger.info("Max input size = %d B (pilot default)" % _maxinputsize)

    return _maxinputsize
