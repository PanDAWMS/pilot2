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
from sys import stderr

try:
    import Queue as queue
except Exception:
    import queue  # python 3

from collections import namedtuple

from pilot.control import job, payload, data, monitor
from pilot.util.constants import SUCCESS
from pilot.common.exception import ExcThread

import logging
logger = logging.getLogger(__name__)


def interrupt(args, signum, frame):
    logger.info('caught signal: %s' % [v for v, k in signal.__dict__.iteritems() if k == signum][0])
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

    logger.info('setting up queues')

    queues = namedtuple('queues', ['jobs', 'payloads', 'data_in', 'data_out',
                                   'validated_jobs', 'validated_payloads', 'monitored_payloads',
                                   'finished_jobs', 'finished_payloads', 'finished_data_in', 'finished_data_out',
                                   'failed_jobs', 'failed_payloads', 'failed_data_in', 'failed_data_out',
                                   'completed_jobs'])

    queues.jobs = queue.Queue()
    queues.payloads = queue.Queue()
    queues.data_in = queue.Queue()
    queues.data_out = queue.Queue()

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
                    'nr_jobs': 0}

    # define the threads
    targets = {'job': job.control, 'payload': payload.control, 'data': data.control, 'monitor': monitor.control}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in targets.items()]

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

    logger.info('end of generic workflow')

    return traces
