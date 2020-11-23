#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

# This module contains implementations of job monitoring tasks

import os
import time
from subprocess import PIPE
from glob import glob

from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import set_pilot_state, show_memory_usage
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import get_directory_size, remove_files, get_local_file_size
from pilot.util.loopingjob import looping_job
from pilot.util.math import convert_mb_to_b, human2bytes
from pilot.util.parameters import convert_to_int, get_maximum_input_sizes
from pilot.util.processes import get_current_cpu_consumption_time, kill_processes, get_number_of_child_processes
from pilot.util.workernode import get_local_disk_space, check_hz

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def job_monitor_tasks(job, mt, args):
    """
    Perform the tasks for the job monitoring.
    The function is called once a minute. Individual checks will be performed at any desired time interval (>= 1
    minute).

    :param job: job object.
    :param mt: `MonitoringTime` object.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    current_time = int(time.time())

    # update timing info for running jobs (to avoid an update after the job has finished)
    if job.state == 'running':
        # confirm that the worker node has a proper SC_CLK_TCK (problems seen on MPPMU)
        check_hz()
        try:
            cpuconsumptiontime = get_current_cpu_consumption_time(job.pid)
        except Exception as e:
            diagnostics = "Exception caught: %s" % e
            logger.warning(diagnostics)
            import traceback
            logger.warning(traceback.format_exc())
            if "Resource temporarily unavailable" in diagnostics:
                exit_code = errors.RESOURCEUNAVAILABLE
            elif "No such file or directory" in diagnostics:
                exit_code = errors.STATFILEPROBLEM
            elif "No such process" in diagnostics:
                exit_code = errors.NOSUCHPROCESS
            else:
                exit_code = errors.GENERALCPUCALCPROBLEM
            return exit_code, diagnostics
        else:
            job.cpuconsumptiontime = int(round(cpuconsumptiontime))
            job.cpuconsumptionunit = "s"
            job.cpuconversionfactor = 1.0
            logger.info('CPU consumption time for pid=%d: %f (rounded to %d)' % (job.pid, cpuconsumptiontime, job.cpuconsumptiontime))

        # check how many cores the payload is using
        set_number_used_cores(job)

        # check memory usage (optional) for jobs in running state
        exit_code, diagnostics = verify_memory_usage(current_time, mt, job)
        if exit_code != 0:
            return exit_code, diagnostics

    # is it time to verify the pilot running time?
#    exit_code, diagnostics = verify_pilot_running_time(current_time, mt, job)
#    if exit_code != 0:
#        return exit_code, diagnostics

    # should the proxy be verified?
    if args.verify_proxy:
        exit_code, diagnostics = verify_user_proxy(current_time, mt)
        if exit_code != 0:
            return exit_code, diagnostics

    # is it time to check for looping jobs?
    exit_code, diagnostics = verify_looping_job(current_time, mt, job)
    if exit_code != 0:
        return exit_code, diagnostics

    # is the job using too much space?
    exit_code, diagnostics = verify_disk_usage(current_time, mt, job)
    if exit_code != 0:
        return exit_code, diagnostics

    # is it time to verify the number of running processes?
    if job.pid:
        exit_code, diagnostics = verify_running_processes(current_time, mt, job.pid)
        if exit_code != 0:
            return exit_code, diagnostics

    # make sure that any utility commands are still running
    if job.utilities != {}:
        utility_monitor(job)

    return exit_code, diagnostics


def set_number_used_cores(job):
    """
    Set the number of cores used by the payload.
    The number of actual used cores is reported with job metrics (if set).

    :param job: job object.
    :return:
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    cpu = __import__('pilot.user.%s.cpu' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
    cpu.set_core_counts(job)


def verify_memory_usage(current_time, mt, job):
    """
    Verify the memory usage (optional).
    Note: this function relies on a stand-alone memory monitor tool that may be executed by the Pilot.

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :param job: job object.
    :return: exit code (int), error diagnostics (string).
    """

    show_memory_usage()

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    memory = __import__('pilot.user.%s.memory' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

    if not memory.allow_memory_usage_verifications():
        return 0, ""

    # is it time to verify the memory usage?
    memory_verification_time = convert_to_int(config.Pilot.memory_usage_verification_time, default=60)
    if current_time - mt.get('ct_memory') > memory_verification_time:
        # is the used memory within the allowed limit?
        try:
            exit_code, diagnostics = memory.memory_usage(job)
        except Exception as e:
            logger.warning('caught exception: %s' % e)
            exit_code = -1
        if exit_code != 0:
            logger.warning('ignoring failure to parse memory monitor output')
            #return exit_code, diagnostics
        else:
            # update the ct_proxy with the current time
            mt.update('ct_memory')

    return 0, ""


def verify_user_proxy(current_time, mt):
    """
    Verify the user proxy.
    This function is called by the job_monitor_tasks() function.

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :return: exit code (int), error diagnostics (string).
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    userproxy = __import__('pilot.user.%s.proxy' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

    # is it time to verify the proxy?
    proxy_verification_time = convert_to_int(config.Pilot.proxy_verification_time, default=600)
    if current_time - mt.get('ct_proxy') > proxy_verification_time:
        # is the proxy still valid?
        exit_code, diagnostics = userproxy.verify_proxy()
        if exit_code != 0:
            return exit_code, diagnostics
        else:
            # update the ct_proxy with the current time
            mt.update('ct_proxy')

    return 0, ""


def verify_looping_job(current_time, mt, job):
    """
    Verify that the job is not looping.

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :param job: job object.
    :return: exit code (int), error diagnostics (string).
    """

    looping_verification_time = convert_to_int(config.Pilot.looping_verification_time, default=600)
    if current_time - mt.get('ct_looping') > looping_verification_time:
        # is the job looping?
        try:
            exit_code, diagnostics = looping_job(job, mt)
        except Exception as e:
            diagnostics = 'exception caught in looping job algorithm: %s' % e
            logger.warning(diagnostics)
            if "No module named" in diagnostics:
                exit_code = errors.BLACKHOLE
            else:
                exit_code = errors.UNKNOWNEXCEPTION
            return exit_code, diagnostics
        else:
            if exit_code != 0:
                return exit_code, diagnostics

        # update the ct_proxy with the current time
        mt.update('ct_looping')

    return 0, ""


def verify_disk_usage(current_time, mt, job):
    """
    Verify the disk usage.
    The function checks 1) payload stdout size, 2) local space, 3) work directory size, 4) output file sizes.

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :param job: job object.
    :return: exit code (int), error diagnostics (string).
    """

    disk_space_verification_time = convert_to_int(config.Pilot.disk_space_verification_time, default=300)
    if current_time - mt.get('ct_diskspace') > disk_space_verification_time:
        # time to check the disk space

        # check the size of the payload stdout
        exit_code, diagnostics = check_payload_stdout(job)
        if exit_code != 0:
            return exit_code, diagnostics

        # check the local space, if it's enough left to keep running the job
        exit_code, diagnostics = check_local_space()
        if exit_code != 0:
            return exit_code, diagnostics

        # check the size of the workdir
        exit_code, diagnostics = check_work_dir(job)
        if exit_code != 0:
            return exit_code, diagnostics

        # check the output file sizes
        exit_code, diagnostics = check_output_file_sizes(job)
        if exit_code != 0:
            return exit_code, diagnostics

        # update the ct_diskspace with the current time
        mt.update('ct_diskspace')

    return 0, ""


def verify_running_processes(current_time, mt, pid):
    """
    Verify the number of running processes.
    The function sets the environmental variable PILOT_MAXNPROC to the maximum number of found (child) processes
    corresponding to the main payload process id.
    The function does not return an error code (always returns exit code 0).

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :param pid: payload process id (int).
    :return: exit code (int), error diagnostics (string).
    """

    nproc_env = 0

    process_verification_time = convert_to_int(config.Pilot.process_verification_time, default=300)
    if current_time - mt.get('ct_process') > process_verification_time:
        # time to check the number of processes
        nproc = get_number_of_child_processes(pid)
        try:
            nproc_env = int(os.environ.get('PILOT_MAXNPROC', 0))
        except Exception as e:
            logger.warning('failed to convert PILOT_MAXNPROC to int: %s' % e)
        else:
            if nproc > nproc_env:
                # set the maximum number of found processes
                os.environ['PILOT_MAXNPROC'] = str(nproc)

        if nproc_env > 0:
            logger.info('maximum number of monitored processes: %d' % nproc_env)

    return 0, ""


def utility_monitor(job):
    """
    Make sure that any utility commands are still running.
    In case a utility tool has crashed, this function may restart the process.
    The function is used by the job monitor thread.

    :param job: job object.
    :return:
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    usercommon = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

    # loop over all utilities
    for utcmd in list(job.utilities.keys()):  # E.g. utcmd = MemoryMonitor, Python 2/3

        # make sure the subprocess is still running
        utproc = job.utilities[utcmd][0]
        if not utproc.poll() is None:
            if job.state == 'finished' or job.state == 'failed' or job.state == 'stageout':
                logger.debug('no need to restart utility command since payload has finished running')
                continue

            # if poll() returns anything but None it means that the subprocess has ended - which it
            # should not have done by itself
            utility_subprocess_launches = job.utilities[utcmd][1]
            if utility_subprocess_launches <= 5:
                logger.warning('detected crashed utility subprocess - will restart it')
                utility_command = job.utilities[utcmd][2]

                try:
                    proc1 = execute(utility_command, workdir=job.workdir, returnproc=True, usecontainer=False,
                                    stdout=PIPE, stderr=PIPE, cwd=job.workdir, queuedata=job.infosys.queuedata)
                except Exception as e:
                    logger.error('could not execute: %s' % e)
                else:
                    # store process handle in job object, and keep track on how many times the
                    # command has been launched
                    job.utilities[utcmd] = [proc1, utility_subprocess_launches + 1, utility_command]
            else:
                logger.warning('detected crashed utility subprocess - too many restarts, will not restart %s again' % utcmd)
        else:  # check the utility output (the selector option adds a substring to the output file name)
            filename = usercommon.get_utility_command_output_filename(utcmd, selector=True)
            path = os.path.join(job.workdir, filename)
            if os.path.exists(path):
                logger.info('file: %s exists' % path)
            else:
                logger.warning('file: %s does not exist' % path)

            # rest
            time.sleep(10)


def get_local_size_limit_stdout(bytes=True):
    """
    Return a proper value for the local size limit for payload stdout (from config file).

    :param bytes: boolean (if True, convert kB to Bytes).
    :return: size limit (int).
    """

    try:
        localsizelimit_stdout = int(config.Pilot.local_size_limit_stdout)
    except Exception as e:
        localsizelimit_stdout = 2097152
        logger.warning('bad value in config for local_size_limit_stdout: %s (will use value: %d kB)' %
                       (e, localsizelimit_stdout))

    # convert from kB to B
    if bytes:
        localsizelimit_stdout *= 1024

    return localsizelimit_stdout


def check_payload_stdout(job):
    """
    Check the size of the payload stdout.

    :param job: job object.
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    # get list of log files
    file_list = glob(os.path.join(job.workdir, 'log.*'))

    # is this a multi-trf job?
    n_jobs = job.jobparams.count("\n") + 1
    for _i in range(n_jobs):
        # get name of payload stdout file created by the pilot
        _stdout = config.Payload.payloadstdout
        if n_jobs > 1:
            _stdout = _stdout.replace(".txt", "_%d.txt" % (_i + 1))

        # add the primary stdout file to the fileList
        file_list.append(os.path.join(job.workdir, _stdout))

    # now loop over all files and check each individually (any large enough file will fail the job)
    for filename in file_list:

        if "job.log.tgz" in filename:
            logger.info("skipping file size check of file (%s) since it is a special log file" % (filename))
            continue

        if os.path.exists(filename):
            try:
                # get file size in bytes
                fsize = os.path.getsize(filename)
            except Exception as e:
                logger.warning("could not read file size of %s: %s" % (filename, e))
            else:
                # is the file too big?
                localsizelimit_stdout = get_local_size_limit_stdout()
                if fsize > localsizelimit_stdout:
                    exit_code = errors.STDOUTTOOBIG
                    diagnostics = "Payload stdout file too big: %d B (larger than limit %d B)" % \
                                  (fsize, localsizelimit_stdout)
                    logger.warning(diagnostics)

                    # kill the job
                    set_pilot_state(job=job, state="failed")
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code)
                    kill_processes(job.pid)

                    # remove the payload stdout file after the log extracts have been created

                    # remove any lingering input files from the work dir
                    lfns, guids = job.get_lfns_and_guids()
                    if lfns:
                        # remove any lingering input files from the work dir
                        exit_code = remove_files(job.workdir, lfns)
                else:
                    logger.info("payload stdout (%s) within allowed size limit (%d B): %d B" % (_stdout, localsizelimit_stdout, fsize))
        else:
            logger.info("skipping file size check of payload stdout file (%s) since it has not been created yet" % _stdout)

    return exit_code, diagnostics


def check_local_space():
    """
    Do we have enough local disk space left to run the job?

    :return: pilot error code (0 if success, NOLOCALSPACE if failure)
    """

    ec = 0
    diagnostics = ""

    # is there enough local space to run a job?
    cwd = os.getcwd()
    logger.debug('checking local space on %s' % cwd)
    spaceleft = convert_mb_to_b(get_local_disk_space(cwd))  # B (diskspace is in MB)
    free_space_limit = human2bytes(config.Pilot.free_space_limit)
    if spaceleft <= free_space_limit:
        diagnostics = 'too little space left on local disk to run job: %d B (need > %d B)' %\
                      (spaceleft, free_space_limit)
        ec = errors.NOLOCALSPACE
        logger.warning(diagnostics)
    else:
        logger.info('sufficient remaining disk space (%d B)' % spaceleft)

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

    if os.path.exists(job.workdir):
        # get the limit of the workdir
        maxwdirsize = get_max_allowed_work_dir_size(job.infosys.queuedata)

        if os.path.exists(job.workdir):
            workdirsize = get_directory_size(directory=job.workdir)

            # is user dir within allowed size limit?
            if workdirsize > maxwdirsize:
                exit_code = errors.USERDIRTOOLARGE
                diagnostics = "work directory (%s) is too large: %d B (must be < %d B)" % \
                              (job.workdir, workdirsize, maxwdirsize)
                logger.fatal("%s" % diagnostics)

                cmd = 'ls -altrR %s' % job.workdir
                _ec, stdout, stderr = execute(cmd, mute=True)
                logger.info("%s: %s" % (cmd + '\n', stdout))

                # kill the job
                # pUtil.createLockFile(True, self.__env['jobDic'][k][1].workdir, lockfile="JOBWILLBEKILLED")
                set_pilot_state(job=job, state="failed")
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code)
                kill_processes(job.pid)

                # remove any lingering input files from the work dir
                lfns, guids = job.get_lfns_and_guids()
                if lfns:
                    remove_files(job.workdir, lfns)

                    # remeasure the size of the workdir at this point since the value is stored below
                    workdirsize = get_directory_size(directory=job.workdir)
            else:
                logger.info("size of work directory %s: %d B (within %d B limit)" % (job.workdir, workdirsize, maxwdirsize))

            # Store the measured disk space (the max value will later be sent with the job metrics)
            if workdirsize > 0:
                job.add_workdir_size(workdirsize)
        else:
            logger.warning('job work dir does not exist: %s' % job.workdir)
    else:
        logger.warning('skipping size check of workdir since it has not been created yet')

    return exit_code, diagnostics


def get_max_allowed_work_dir_size(queuedata):
    """
    Return the maximum allowed size of the work directory.

    :param queuedata: job.infosys.queuedata object.
    :return: max allowed work dir size in Bytes (int).
    """

    try:
        maxwdirsize = convert_mb_to_b(get_maximum_input_sizes())  # from MB to B, e.g. 16336 MB -> 17,129,537,536 B
    except Exception as e:
        max_input_size = get_max_input_size()
        maxwdirsize = max_input_size + config.Pilot.local_size_limit_stdout * 1024
        logger.info("work directory size check will use %d B as a max limit (maxinputsize [%d B] + local size limit for"
                    " stdout [%d B])" % (maxwdirsize, max_input_size, config.Pilot.local_size_limit_stdout * 1024))
        logger.warning('conversion caught exception: %s' % e)
    else:
        # grace margin, as discussed in https://its.cern.ch/jira/browse/ATLASPANDA-482
        margin = 10.0  # percent, read later from somewhere
        maxwdirsize = int(maxwdirsize * (1 + margin / 100.0))
        logger.info("work directory size check will use %d B as a max limit (10%% grace limit added)" % maxwdirsize)

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


def check_output_file_sizes(job):
    """
    Are the output files within the allowed size limits?

    :param job: job object.
    :return: exit code (int), error diagnostics (string)
    """

    exit_code = 0
    diagnostics = ""

    # loop over all known output files
    for fspec in job.outdata:
        path = os.path.join(job.workdir, fspec.lfn)
        if os.path.exists(path):
            # get the current file size
            fsize = get_local_file_size(path)
            max_fsize = human2bytes(config.Pilot.maximum_output_file_size)
            if fsize and fsize < max_fsize:
                logger.info('output file %s is within allowed size limit (%d B < %d B)' % (path, fsize, max_fsize))
            else:
                exit_code = errors.OUTPUTFILETOOLARGE
                diagnostics = 'output file %s is not within allowed size limit (%d B > %d B)' % (path, fsize, max_fsize)
                logger.warning(diagnostics)
        else:
            logger.info('output file size check: skipping output file %s since it does not exist' % path)

    return exit_code, diagnostics
