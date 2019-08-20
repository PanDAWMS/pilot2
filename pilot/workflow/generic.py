#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2018

from __future__ import print_function

import functools
import signal
import threading
from time import time, sleep
from sys import stderr
from os import getpid
from shutil import rmtree

try:
    import Queue as queue  # noqa: N813
except Exception:
    import queue  # python 3

from collections import namedtuple

from pilot.common.exception import ExcThread
from pilot.control import job, payload, data, monitor
from pilot.util.constants import SUCCESS, PILOT_KILL_SIGNAL, MAX_KILL_WAIT_TIME
from pilot.util.processes import kill_processes
from pilot.util.timing import add_to_pilot_timing

import logging
logger = logging.getLogger(__name__)


def interrupt(args, signum, frame):
    """
    Interrupt function on the receiving end of kill signals.
    This function is forwarded any incoming signals (SIGINT, SIGTERM, etc) and will set abort_job which instructs
    the threads to abort the job.

    :param args: pilot arguments.
    :param signum: signal.
    :param frame: stack/execution frame pointing to the frame that was interrupted by the signal.
    :return:
    """

    sig = [v for v, k in signal.__dict__.iteritems() if k == signum][0]
    args.signal_counter += 1

    # keep track of when first kill signal arrived, any stuck loops should abort at a defined cut off time
    if args.kill_time == 0:
        args.kill_time = int(time())

    max_kill_wait_time = MAX_KILL_WAIT_TIME + 60  # add another minute of grace to let threads finish
    current_time = int(time())
    if args.kill_time and current_time - args.kill_time > max_kill_wait_time:
        logger.warning('passed maximum waiting time after first kill signal - will commit suicide - farewell')
        try:
            rmtree(args.sourcedir)
        except Exception as e:
            logger.warning(e)
        logging.shutdown()
        kill_processes(getpid())

    add_to_pilot_timing('0', PILOT_KILL_SIGNAL, time(), args)
    logger.warning('caught signal: %s' % sig)
    args.signal = sig
    logger.warning('will instruct threads to abort and update the server')
    args.abort_job.set()
    logger.warning('waiting for threads to finish')
    args.job_aborted.wait()
    logger.warning('setting graceful stop (in case it was not set already), pilot will abort')
    args.graceful_stop.set()


def run(args):
    """
    Main execution function for the generic workflow.

    The function sets up the internal queues which handle the flow of jobs.

    :param args: pilot arguments.
    :returns: traces.
    """

    logger.info('setting up signal handling')
    signal.signal(signal.SIGINT, functools.partial(interrupt, args))
    signal.signal(signal.SIGTERM, functools.partial(interrupt, args))
    signal.signal(signal.SIGQUIT, functools.partial(interrupt, args))
    signal.signal(signal.SIGSEGV, functools.partial(interrupt, args))
    signal.signal(signal.SIGXCPU, functools.partial(interrupt, args))
    signal.signal(signal.SIGUSR1, functools.partial(interrupt, args))
    signal.signal(signal.SIGBUS, functools.partial(interrupt, args))

    logger.info('setting up queues')
    queues = namedtuple('queues', ['jobs', 'payloads', 'data_in', 'data_out', 'current_data_in',
                                   'validated_jobs', 'validated_payloads', 'monitored_payloads',
                                   'finished_jobs', 'finished_payloads', 'finished_data_in', 'finished_data_out',
                                   'failed_jobs', 'failed_payloads', 'failed_data_in', 'failed_data_out',
                                   'completed_jobs'])

    queues.jobs = queue.Queue()
    queues.payloads = queue.Queue()
    queues.data_in = queue.Queue()
    queues.data_out = queue.Queue()

    queues.current_data_in = queue.Queue()
    queues.validated_jobs = queue.Queue()
    queues.validated_payloads = queue.Queue()
    queues.monitored_payloads = queue.Queue()

    queues.finished_jobs = queue.Queue()
    queues.finished_payloads = queue.Queue()
    queues.finished_data_in = queue.Queue()
    queues.finished_data_out = queue.Queue()

    queues.failed_jobs = queue.Queue()
    queues.failed_payloads = queue.Queue()
    queues.failed_data_in = queue.Queue()
    queues.failed_data_out = queue.Queue()

    queues.completed_jobs = queue.Queue()

    logger.info('setting up tracing')
    traces = namedtuple('traces', ['pilot'])
    traces.pilot = {'state': SUCCESS,
                    'nr_jobs': 0,
                    'error_code': 0,
                    'command': None}

    # initial sanity check defined by pilot user
    try:
        user = __import__('pilot.user.%s.common' % args.pilot_user.lower(), globals(), locals(), [args.pilot_user.lower()], -1)
        exit_code = user.sanity_check()
    except Exception as e:
        logger.info('skipping sanity check since: %s' % e)
    else:
        if exit_code != 0:
            logger.info('aborting workflow since sanity check failed')
            traces.pilot['error_code'] = exit_code
            return traces
        else:
            logger.info('passed sanity check')

    # define the threads
    targets = {'job': job.control, 'payload': payload.control, 'data': data.control, 'monitor': monitor.control}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in targets.items()]

    logger.info('starting threads')
    [thread.start() for thread in threads]

    logger.info('waiting for interrupts')

    thread_count = threading.activeCount()
    while threading.activeCount() > 1:
        for thread in threads:
            bucket = thread.get_bucket()
            try:
                exc = bucket.get(block=False)
            except queue.Empty:
                pass
            else:
                exc_type, exc_obj, exc_trace = exc
                # deal with the exception
                print('received exception from bucket queue in generic workflow: %s' % exc_obj, file=stderr)
                # logger.fatal('caught exception: %s' % exc_obj)

            thread.join(0.1)

        abort = False
        if thread_count != threading.activeCount():
            thread_count = threading.activeCount()
            logger.debug('thread count now at %d threads' % thread_count)
            logger.debug('enumerate: %s' % str(threading.enumerate()))
            # count all non-daemon threads
            daemon_threads = 0
            for thread in threading.enumerate():
                if thread.isDaemon():  # ignore any daemon threads, they will be aborted when python ends
                    daemon_threads += 1
            if thread_count - daemon_threads == 1:
                logger.debug('aborting since there is[are] %d daemon thread[s] which can be ignored' % daemon_threads)
                abort = True

        if abort:
            break

        sleep(0.1)

    logger.info('end of generic workflow (traces error code: %d)' % traces.pilot['error_code'])

    return traces
