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

import json
import os
import time
import traceback

try:
    import Queue as queue
except Exception:
    import queue  # python 3

from pilot.control.payloads import generic, eventservice, eventservicemerge
from pilot.control.job import send_state
from pilot.util.processes import get_cpu_consumption_time
from pilot.util.config import config
from pilot.util.filehandling import read_file, get_guid
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
            queues.validated_payloads.put(job)
        else:
            queues.failed_payloads.put(job)


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


def get_payload_executor(args, job, out, err):
    """
    Get payload executor function for different payload.

    :param args:
    :param job:
    :param out:
    :param err:
    :return: instance of a payload executor
    """
    if job.is_eventservice:
        payload_executor = eventservice.Executor(args, job, out, err)
    elif job.is_eventservicemerge:
        payload_executor = eventservicemerge.Executor(args, job, out, err)
    else:
        payload_executor = generic.Executor(args, job, out, err)
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
            log = logger.getChild(job.jobid)

            q_snapshot = list(queues.finished_data_in.queue)
            peek = [s_job for s_job in q_snapshot if job.jobid == s_job.jobid]
            if len(peek) == 0:
                queues.validated_payloads.put(job)
                for i in xrange(10):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(0.1)
                continue

            # this job is now to be monitored, so add it to the monitored_payloads queue
            queues.monitored_payloads.put(job)

            log.info('job %s added to monitored payloads queue' % job.jobid)

            out = open(os.path.join(job.workdir, config.Payload.payloadstdout), 'wb')
            err = open(os.path.join(job.workdir, config.Payload.payloadstderr), 'wb')

            send_state(job, args, 'starting')

            payload_executor = get_payload_executor(args, job, out, err)
            log.info("Got payload executor: %s" % payload_executor)

            # run the payload and measure the execution time
            job.t0 = os.times()
            exit_code = payload_executor.run()

            cpuconsumptiontime = get_cpu_consumption_time(job.t0)
            job.cpuconsumptiontime = int(round(cpuconsumptiontime))
            job.cpuconsumptionunit = "s"
            job.cpuconversionfactor = 1.0
            log.info('CPU consumption time: %f %s (rounded to %d %s)' %
                     (cpuconsumptiontime, job.cpuconsumptionunit, job.cpuconsumptiontime, job.cpuconsumptionunit))

            out.close()
            err.close()

            if exit_code == 0:
                job.transexitcode = 0
                queues.finished_payloads.put(job)
            else:
                stderr = read_file(os.path.join(job.workdir, config.Payload.payloadstderr))
                if stderr != "":
                    msg = errors.extract_stderr_msg(stderr)
                    if msg != "":
                        log.warning("extracted message from stderr:\n%s" % msg)
                ec = errors.resolve_transform_error(exit_code, stderr)
                if ec != 0:
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(ec)
                job.transexitcode = exit_code
                queues.failed_payloads.put(job)

        except queue.Empty:
            continue
        except Exception as e:
            logger.fatal('execute payloads caught an exception (cannot recover): %s, %s' % (e, traceback.format_exc()))
            if job:
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXECUTIONEXCEPTION)
                queues.failed_payloads.put(job)
            while not args.graceful_stop.is_set():
                # let stage-out of log finish, but stop running payloads as there should be a problem with the pilot
                time.sleep(5)


def process_job_report(job):
    """
    Process the job report produced by the payload/transform if it exists.
    Payload error codes and diagnostics, as well as payload metadata (for output files) and stageout type will be
    extracted. The stageout type is either "all" (i.e. stage-out both output and log files) or "log" (i.e. only log file
    will be staged out).
    Note: some fields might be experiment specific. A call to a user function is therefore also done.

    :param job: job dictionary will be updated by the function and several fields set.
    :return:
    """

    log = logger.getChild(job.jobid)
    path = os.path.join(job.workdir, config.Payload.jobreport)
    if not os.path.exists(path):
        log.warning('job report does not exist: %s (any missing output file guids must be generated)' % path)

        # add missing guids
        for dat in job.outdata:
            if not dat.guid:
                dat.guid = get_guid()
                log.warning('guid not set: generated guid=%s for lfn=%s' % (dat.guid, dat.lfn))

    else:
        with open(path) as data_file:
            # compulsory field; the payload must produce a job report (see config file for file name)
            job.metadata = json.load(data_file)

            # extract user specific info from job report
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], -1)
            user.update_job_data(job)

            # compulsory fields
            try:
                job.exitcode = job.metadata['exitCode']
            except Exception as e:
                log.warning('could not find compulsory payload exitCode in job report: %s (will be set to 0)' % e)
                job.exitcode = 0
            else:
                log.info('extracted exit code from job report: %d' % job.exitcode)
            try:
                job.exitmsg = job.metadata['exitMsg']
            except Exception as e:
                log.warning('could not find compulsory payload exitMsg in job report: %s (will be set to empty string)' % e)
                job.exitmsg = ""
            else:
                log.info('extracted exit message from job report: %s' % job.exitmsg)


def validate_post(queues, traces, args):
    """
    Validate finished payloads.
    If payload finished correctly, add the job to the data_out queue. If it failed, add it to the data_out queue as
    well but only for log stage-out.

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
            continue
        log = logger.getChild(job.jobid)

        # by default, both output and log should be staged out
        job.stageout = 'all'

        # process the job report if it exists and set multiple fields
        process_job_report(job)

        log.debug('adding job to data_out queue')
        queues.data_out.put(job)


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
        log = logger.getChild(job.jobid)

        log.debug('adding log for log stageout')

        job.stageout = "log"  # only stage-out log file
        queues.data_out.put(job)
