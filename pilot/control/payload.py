#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2020
# - Wen Guan, wen.guan@cern.ch, 2017-2018

import os
import time
import traceback

try:
    import Queue as queue  # noqa: N813
except Exception:
    import queue  # Python 3

from pilot.control.payloads import generic, eventservice, eventservicemerge
from pilot.control.job import send_state
from pilot.util.auxiliary import set_pilot_state, show_memory_usage
from pilot.util.processes import get_cpu_consumption_time
from pilot.util.config import config
from pilot.util.filehandling import read_file, remove_core_dumps
from pilot.util.processes import threads_aborted
from pilot.util.queuehandling import put_in_queue
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import ExcThread

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def control(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    targets = {'validate_pre': validate_pre, 'execute_payloads': execute_payloads, 'validate_post': validate_post,
               'failed_post': failed_post}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in list(targets.items())]  # Python 3

    [thread.start() for thread in threads]

    # if an exception is thrown, the graceful_stop will be set by the ExcThread class run() function
    while not args.graceful_stop.is_set():
        for thread in threads:
            bucket = thread.get_bucket()
            try:
                exc = bucket.get(block=False)
            except queue.Empty:
                pass
            else:
                exc_type, exc_obj, exc_trace = exc
                logger.warning("thread \'%s\' received an exception from bucket: %s" % (thread.name, exc_obj))

                # deal with the exception
                # ..

            thread.join(0.1)
            time.sleep(0.1)

        time.sleep(0.5)

    logger.debug('payload control ending since graceful_stop has been set')
    if args.abort_job.is_set():
        if traces.pilot['command'] == 'aborting':
            logger.warning('jobs are aborting')
        elif traces.pilot['command'] == 'abort':
            logger.warning('data control detected a set abort_job (due to a kill signal)')
            traces.pilot['command'] = 'aborting'

            # find all running jobs and stop them, find all jobs in queues relevant to this module
            #abort_jobs_in_queues(queues, args.signal)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[payload] control thread has finished')


def validate_pre(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.payloads.get(block=True, timeout=1)
        except queue.Empty:
            continue

        if _validate_payload(job):
            #queues.validated_payloads.put(job)
            put_in_queue(job, queues.validated_payloads)
        else:
            #queues.failed_payloads.put(job)
            put_in_queue(job, queues.failed_payloads)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] validate_pre thread has finished')


def _validate_payload(job):
    """
    Perform validation tests for the payload.

    :param job: job object.
    :return: boolean.
    """

    status = True

    # perform user specific validation
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
    try:
        status = user.validate(job)
    except Exception as e:
        logger.fatal('failed to execute user validate() function: %s' % e)
        status = False

    return status


def get_payload_executor(args, job, out, err, traces):
    """
    Get payload executor function for different payload.

    :param args: args object.
    :param job: job object.
    :param out:
    :param err:
    :param traces: traces object.
    :return: instance of a payload executor
    """
    if job.is_eventservice:  # True for native HPO workflow as well
        payload_executor = eventservice.Executor(args, job, out, err, traces)
    elif job.is_eventservicemerge:
        payload_executor = eventservicemerge.Executor(args, job, out, err, traces)
    else:
        payload_executor = generic.Executor(args, job, out, err, traces)
    return payload_executor


def execute_payloads(queues, traces, args):  # noqa: C901
    """
    Execute queued payloads.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    job = None
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.validated_payloads.get(block=True, timeout=1)

            q_snapshot = list(queues.finished_data_in.queue)
            peek = [s_job for s_job in q_snapshot if job.jobid == s_job.jobid]
            if len(peek) == 0:
                #queues.validated_payloads.put(job)
                put_in_queue(job, queues.validated_payloads)
                for i in range(10):  # Python 3
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(1)
                continue

            # this job is now to be monitored, so add it to the monitored_payloads queue
            #queues.monitored_payloads.put(job)
            put_in_queue(job, queues.monitored_payloads)

            logger.info('job %s added to monitored payloads queue' % job.jobid)

            try:
                out = open(os.path.join(job.workdir, config.Payload.payloadstdout), 'wb')
                err = open(os.path.join(job.workdir, config.Payload.payloadstderr), 'wb')
            except Exception as e:
                logger.warning('failed to open payload stdout/err: %s' % e)
                out = None
                err = None
            send_state(job, args, 'starting')

            # note: when sending a state change to the server, the server might respond with 'tobekilled'
            if job.state == 'failed':
                logger.warning('job state is \'failed\' - abort execute_payloads()')
                break

            payload_executor = get_payload_executor(args, job, out, err, traces)
            logger.info("Got payload executor: %s" % payload_executor)

            show_memory_usage()

            # run the payload and measure the execution time
            job.t0 = os.times()
            exit_code = payload_executor.run()

            set_cpu_consumption_time(job)
            job.transexitcode = exit_code % 255

            out.close()
            err.close()

            #if traces.pilot['nr_jobs'] == 1:
            #    logger.debug('faking job failure in first multi-job')
            #    job.transexitcode = 1
            #    exit_code = 1

            # analyze and interpret the payload execution output
            perform_initial_payload_error_analysis(job, exit_code)

            # was an error already found?
            #if job.piloterrorcodes:
            #    exit_code_interpret = 1
            #else:
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            user = __import__('pilot.user.%s.diagnose' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
            try:
                exit_code_interpret = user.interpret(job)
            except Exception as e:
                logger.warning('exception caught: %s' % e)
                #exit_code_interpret = -1
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.INTERNALPILOTPROBLEM)

            if job.piloterrorcodes:
                exit_code_interpret = 1

            if exit_code_interpret == 0 and exit_code == 0:
                logger.info('main payload error analysis completed - did not find any errors')

                # update output lists if zipmaps were used
                #job.add_archives_to_output_lists()

                # queues.finished_payloads.put(job)
                put_in_queue(job, queues.finished_payloads)
            else:
                logger.debug('main payload error analysis completed - adding job to failed_payloads queue')
                #queues.failed_payloads.put(job)
                put_in_queue(job, queues.failed_payloads)

        except queue.Empty:
            continue
        except Exception as e:
            logger.fatal('execute payloads caught an exception (cannot recover): %s, %s' % (e, traceback.format_exc()))
            if job:
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXECUTIONEXCEPTION)
                #queues.failed_payloads.put(job)
                put_in_queue(job, queues.failed_payloads)
            while not args.graceful_stop.is_set():
                # let stage-out of log finish, but stop running payloads as there should be a problem with the pilot
                time.sleep(5)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] execute_payloads thread has finished')


def set_cpu_consumption_time(job):
    """
    Set the CPU consumption time.
    :param job: job object.
    :return:
    """

    cpuconsumptiontime = get_cpu_consumption_time(job.t0)
    job.cpuconsumptiontime = int(round(cpuconsumptiontime))
    job.cpuconsumptionunit = "s"
    job.cpuconversionfactor = 1.0
    logger.info('CPU consumption time: %f %s (rounded to %d %s)' %
                (cpuconsumptiontime, job.cpuconsumptionunit, job.cpuconsumptiontime, job.cpuconsumptionunit))


def perform_initial_payload_error_analysis(job, exit_code):
    """
    Perform an initial analysis of the payload.
    Singularity errors are caught here.

    :param job: job object.
    :param exit_code: exit code from payload execution.
    :return:
    """

    if exit_code != 0:
        msg = ""
        ec = 0
        logger.warning('main payload execution returned non-zero exit code: %d' % exit_code)
        stderr = read_file(os.path.join(job.workdir, config.Payload.payloadstderr))
        if stderr != "":
            msg = errors.extract_stderr_error(stderr)
            if msg == "":
                # look for warning messages instead (might not be fatal so do not set UNRECOGNIZEDTRFSTDERR)
                msg = errors.extract_stderr_warning(stderr)
                fatal = False
            else:
                fatal = True
            if msg != "":
                logger.warning("extracted message from stderr:\n%s" % msg)
                ec = set_error_code_from_stderr(msg, fatal)

        if not ec:
            ec = errors.resolve_transform_error(exit_code, stderr)
        if ec != 0:
            if msg:
                msg = errors.format_diagnostics(ec, msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(ec, msg=msg)
        else:
            if job.piloterrorcodes:
                logger.warning('error code(s) already set: %s' % str(job.piloterrorcodes))
            else:
                # check if core dumps exist, if so remove them and return True
                if remove_core_dumps(job.workdir):
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.COREDUMP)
                else:
                    logger.warning('initial error analysis did not resolve the issue (and core dumps were not found)')
    else:
        logger.info('main payload execution returned zero exit code, but will check it more carefully')


def set_error_code_from_stderr(msg, fatal):
    """
    Identify specific errors in stderr and set the corresponding error code.
    The function returns 0 if no error is recognized.

    :param msg: stderr (string).
    :param fatal: boolean flag if fatal error among warning messages in stderr.
    :return: error code (int).
    """

    if "Failed invoking the NEWUSER namespace runtime" in msg:
        ec = errors.SINGULARITYNEWUSERNAMESPACE
    elif "Failed to create user namespace" in msg:
        ec = errors.SINGULARITYFAILEDUSERNAMESPACE
    elif "command not found" in msg:
        ec = errors.TRANSFORMNOTFOUND
    elif "SL5 is unsupported" in msg:
        ec = errors.UNSUPPORTEDSL5OS
    elif "resource temporarily unavailable" in msg:
        ec = errors.SINGULARITYRESOURCEUNAVAILABLE
    elif "unrecognized arguments" in msg:
        ec = errors.UNRECOGNIZEDTRFARGUMENTS
    elif fatal:
        ec = errors.UNRECOGNIZEDTRFSTDERR
    else:
        ec = 0

    return ec


def validate_post(queues, traces, args):
    """
    Validate finished payloads.
    If payload finished correctly, add the job to the data_out queue. If it failed, add it to the data_out queue as
    well but only for log stage-out (in failed_post() below).

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        # finished payloads
        try:
            job = queues.finished_payloads.get(block=True, timeout=1)
        except queue.Empty:
            time.sleep(0.1)
            continue

        # by default, both output and log should be staged out
        job.stageout = 'all'
        logger.debug('adding job to data_out queue')
        #queues.data_out.put(job)
        set_pilot_state(job=job, state='stageout')
        put_in_queue(job, queues.data_out)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] validate_post thread has finished')


def failed_post(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        # finished payloads
        try:
            job = queues.failed_payloads.get(block=True, timeout=1)
        except queue.Empty:
            time.sleep(0.1)
            continue

        logger.debug('adding log for log stageout')

        job.stageout = 'log'  # only stage-out log file
        #queues.data_out.put(job)
        set_pilot_state(job=job, state='stageout')
        put_in_queue(job, queues.data_out)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] failed_post thread has finished')
