#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

import Queue
import json
import subprocess
import threading
import time
import shlex
import os

from pilot.util import information
from pilot.control.job import send_state

import logging
logger = logging.getLogger(__name__)


def control(queues, graceful_stop, traces, args):

    threads = [threading.Thread(target=validate_pre,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=execute,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=validate_post,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args})]

    batchqueues = information.get_queues()

    if args.queue not in [queue['name'] for queue in batchqueues]:
        logger.critical('configured queue not found: {0} -- aborting'.format(args.queue))
        graceful_stop.set()
        return

    if not [queue for queue in batchqueues if queue['name'] == args.queue and queue['state'] == 'ACTIVE']:
        logger.critical('configured queue is NOT ACTIVE: {0} -- aborting'.format(args.queue))
        graceful_stop.set()
        return

    logger.info('configured queue: {0}'.format(args.queue))

    [t.start() for t in threads]


def validate_pre(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        if _validate_payload(job):
            queues.validated_payloads.put(job)
        else:
            queues.failed_payloads.put(job)


def _validate_payload(job):
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('payload did not validate correctly -- skipping')
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'payload failed random validation'
    #     return False
    return True


def start_payload(job, out, err):
    log = logger.getChild(job['job_id'])

    executable = ['/usr/bin/env', job['command']] + shlex.split(job['command_parameters'])
    log.debug('executable={0}'.format(executable))

    try:
        proc = subprocess.Popen(executable,
                                bufsize=-1,
                                stdout=out,
                                stderr=err,
                                cwd=job['working_dir'])
    except Exception as e:
        log.error('could not execute: {0}'.format(e))
        return None

    log.info('started -- pid={0} executable={1}'.format(proc.pid, executable))

    return proc


def wait_graceful(proc, graceful_stop, job):
    log = logger.getChild(job['job_id'])

    breaker = False
    exit_code = None
    while True:
        for i in xrange(100):
            if graceful_stop.is_set():
                breaker = True
                log.debug('breaking -- sending SIGTERM pid={0}'.format(proc.pid))
                proc.terminate()
                break
            time.sleep(0.1)
        if breaker:
            log.debug('breaking -- sleep 3s before sending SIGKILL pid={0}'.format(proc.pid))
            time.sleep(3)
            proc.kill()
            break

        exit_code = proc.poll()
        log.info('running: pid={0} exit_code={1}'.format(proc.pid, exit_code))
        if exit_code is not None:
            break
        else:
            send_state(job, 'running')
            continue

    return exit_code


def execute(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.validated_payloads.get(block=True, timeout=1)
            log = logger.getChild(job['job_id'])

            q_snapshot = list(queues.finished_data_in.queue)  # is this for future parallel file transfer?
            peek = [s_job for s_job in q_snapshot if job['job_id'] == s_job['job_id']]
            if len(peek) == 0:
                log.debug('Not yet files received')
                queues.validated_payloads.put(job)
                for i in xrange(10):
                    if graceful_stop.is_set():
                            break
                    time.sleep(0.1)
                continue

            send_state(job, 'starting')

            log.debug('opening payload stdout/err logs')
            out = open(os.path.join(job['working_dir'], 'payload.stdout'), 'wb')
            err = open(os.path.join(job['working_dir'], 'payload.stderr'), 'wb')

            proc = start_payload(job, out, err)
            exit_code = None
            if proc is not None:
                exit_code = wait_graceful(proc, graceful_stop, job)
                log.info('finished pid={0} exit_code={1}'.format(proc.pid, exit_code))

            log.debug('closing payload stdout/err logs')
            out.close()
            err.close()

            if exit_code == 0:
                queues.finished_payloads.put(job)
            else:
                queues.failed_payloads.put(job)

        except Queue.Empty:
            continue


def validate_post(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.finished_payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(job['job_id'])

        log.debug('adding job report for stageout')
        with open(os.path.join(job['working_dir'], 'jobReport.json')) as data_file:
            job['job_report'] = json.load(data_file)

        queues.data_out.put(job)
