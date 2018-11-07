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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2018
# - Wen Guan, wen.guan@cern.ch, 2017-2018

import os
import time
import traceback

try:
    import Queue as queue
except Exception:
    import queue  # python 3

from pilot.control.payloads import generic, eventservice, eventservicemerge
from pilot.control.job import send_state
from pilot.util.auxiliary import get_logger, set_pilot_state
from pilot.util.processes import get_cpu_consumption_time
from pilot.util.config import config
from pilot.util.filehandling import read_file
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
                         name=name) for name, target in targets.items()]

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

    logger.debug('payload control ending since graceful_stop has been set')
    if args.abort_job.is_set():
        if traces.pilot['command'] == 'aborting':
            logger.warning('jobs are aborting')
        elif traces.pilot['command'] == 'abort':
            logger.warning('data control detected a set abort_job (due to a kill signal)')
            traces.pilot['command'] = 'aborting'

            # find all running jobs and stop them, find all jobs in queues relevant to this module
            #abort_jobs_in_queues(queues, args.signal)


def validate_pre(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """
    while not args.graceful_stop.is_set():
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


def _validate_payload(job):
    """
    (add description)

    :param job:
    :return:
    """
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('payload did not validate correctly -- skipping')
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'payload failed random validation'
    #     return False
    return True


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
    if job.is_eventservice:
        payload_executor = eventservice.Executor(args, job, out, err, traces)
    elif job.is_eventservicemerge:
        payload_executor = eventservicemerge.Executor(args, job, out, err, traces)
    else:
        payload_executor = generic.Executor(args, job, out, err, traces)
    return payload_executor


def execute_payloads(queues, traces, args):
    """
    Execute queued payloads.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    job = None
    while not args.graceful_stop.is_set():
        try:
            job = queues.validated_payloads.get(block=True, timeout=1)
            log = get_logger(job.jobid, logger)

            q_snapshot = list(queues.finished_data_in.queue)
            peek = [s_job for s_job in q_snapshot if job.jobid == s_job.jobid]
            if len(peek) == 0:
                #queues.validated_payloads.put(job)
                put_in_queue(job, queues.validated_payloads)
                for i in xrange(10):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(0.1)
                continue

            # this job is now to be monitored, so add it to the monitored_payloads queue
            #queues.monitored_payloads.put(job)
            put_in_queue(job, queues.monitored_payloads)

            log.info('job %s added to monitored payloads queue' % job.jobid)

            out = open(os.path.join(job.workdir, config.Payload.payloadstdout), 'wb')
            err = open(os.path.join(job.workdir, config.Payload.payloadstderr), 'wb')

            send_state(job, args, 'starting')

            payload_executor = get_payload_executor(args, job, out, err, traces)
            log.info("Got payload executor: %s" % payload_executor)

            # run the payload and measure the execution time
            job.t0 = os.times()
            exit_code = payload_executor.run()

            set_cpu_consumption_time(job)
            job.transexitcode = exit_code % 255

            out.close()
            err.close()

            # analyze and interpret the payload execution output
            perform_initial_payload_error_analysis(job, exit_code)
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            user = __import__('pilot.user.%s.diagnose' % pilot_user, globals(), locals(), [pilot_user], -1)
            try:
                exit_code_interpret = user.interpret(job)
            except Exception as e:
                log.warning('exception caught: %s' % e)
                exit_code_interpret = -1
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.INTERNALPILOTPROBLEM)

            if exit_code_interpret == 0 and exit_code == 0:
                log.info('main payload error analysis completed - did not find any errors')
                # queues.finished_payloads.put(job)
                put_in_queue(job, queues.finished_payloads)
            else:
                log.debug('main payload error analysis completed - adding job to failed_payloads queue')
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


def set_cpu_consumption_time(job):
    """
    Set the CPU consumption time.
    :param job: job object.
    :return:
    """

    log = get_logger(job.jobid, logger)

    cpuconsumptiontime = get_cpu_consumption_time(job.t0)
    job.cpuconsumptiontime = int(round(cpuconsumptiontime))
    job.cpuconsumptionunit = "s"
    job.cpuconversionfactor = 1.0
    log.info('CPU consumption time: %f %s (rounded to %d %s)' %
             (cpuconsumptiontime, job.cpuconsumptionunit, job.cpuconsumptiontime, job.cpuconsumptionunit))


def perform_initial_payload_error_analysis(job, exit_code):
    """
    Perform an initial analysis of the payload.
    Singularity errors are caught here.

    :param job: job object.
    :param exit_code: exit code from payload execution.
    :return:
    """

    log = get_logger(job.jobid, logger)

    if exit_code != 0:
        log.warning('main payload execution returned non-zero exit code: %d' % exit_code)
        stderr = read_file(os.path.join(job.workdir, config.Payload.payloadstderr))
        if stderr != "":
            msg = errors.extract_stderr_msg(stderr)
            if msg != "":
                log.warning("extracted message from stderr:\n%s" % msg)
        ec = errors.resolve_transform_error(exit_code, stderr)
        if ec != 0:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(ec)
        else:
            log.warning('initial error analysis did not resolve the issue')
    else:
        log.info('main payload execution returned zero exit code, but will check it more carefully')


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
        # finished payloads
        try:
            job = queues.finished_payloads.get(block=True, timeout=1)
        except queue.Empty:
            time.sleep(0.1)
            continue
        log = get_logger(job.jobid, logger)

        # by default, both output and log should be staged out
        job.stageout = 'all'
        log.debug('adding job to data_out queue')
        #queues.data_out.put(job)
        set_pilot_state(job=job, state='stageout')
        put_in_queue(job, queues.data_out)

    logger.info('validate_post has finished')


def failed_post(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        # finished payloads
        try:
            job = queues.failed_payloads.get(block=True, timeout=1)
        except queue.Empty:
            continue
        log = get_logger(job.jobid, logger)

        log.debug('adding log for log stageout')

        job.stageout = 'log'  # only stage-out log file
        #queues.data_out.put(job)
        set_pilot_state(job=job, state='stageout')
        put_in_queue(job, queues.data_out)
