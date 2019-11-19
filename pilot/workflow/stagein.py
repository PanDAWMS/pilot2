#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

from __future__ import print_function  # Python 2, 2to3 complains about this

import functools
import signal
import threading
from time import time
from sys import stderr

try:
    import Queue as queue  # noqa: N813
except Exception:
    import queue  # Python 3

from collections import namedtuple

from pilot.common.exception import ExcThread
from pilot.control import job, data, monitor
from pilot.util.constants import SUCCESS, PILOT_KILL_SIGNAL
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

    try:
        sig = [v for v, k in signal.__dict__.iteritems() if k == signum][0]  # Python 2
    except Exception:
        sig = [v for v, k in list(signal.__dict__.items()) if k == signum][0]  # Python 3
    add_to_pilot_timing('0', PILOT_KILL_SIGNAL, time(), args)
    add_to_pilot_timing('1', PILOT_KILL_SIGNAL, time(), args)
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
    Main execution function for the stage-in workflow.

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
    queues = namedtuple('queues', ['jobs', 'data_in', 'data_out', 'current_data_in', 'validated_jobs',
                                   'finished_jobs', 'finished_data_in', 'finished_data_out',
                                   'failed_jobs', 'failed_data_in', 'failed_data_out', 'completed_jobs'])

    queues.jobs = queue.Queue()
    queues.data_in = queue.Queue()
    queues.data_out = queue.Queue()

    queues.current_data_in = queue.Queue()
    queues.validated_jobs = queue.Queue()

    queues.finished_jobs = queue.Queue()
    queues.finished_data_in = queue.Queue()
    queues.finished_data_out = queue.Queue()

    queues.failed_jobs = queue.Queue()
    queues.failed_data_in = queue.Queue()
    queues.failed_data_out = queue.Queue()

    queues.completed_jobs = queue.Queue()

    logger.info('setting up tracing')
    traces = namedtuple('traces', ['pilot'])
    traces.pilot = {'state': SUCCESS,
                    'nr_jobs': 0,
                    'error_code': 0,
                    'command': None}

    # define the threads
    targets = {'job': job.control, 'data': data.control, 'monitor': monitor.control}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in list(targets.items())]

    logger.info('starting threads')
    [thread.start() for thread in threads]

    logger.info('waiting for interrupts')

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

    logger.info('end of stage-in workflow (traces error code: %d)' % traces.pilot['error_code'])

    return traces
