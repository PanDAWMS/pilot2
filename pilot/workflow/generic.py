#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

import Queue
import threading

from collections import namedtuple

from pilot.control import job, payload, data, lifetime
from pilot.util.constants import SUCCESS
from pilot.util import signalling

import logging
logger = logging.getLogger(__name__)


def interrupt(signum, frame):
    logger.info('caught ' + signalling.signals_reverse[signum])


def run(args):
    signalling.signal_all_setup(interrupt)

    logger.info('setting up queues')

    queues = namedtuple('queues', ['jobs', 'payloads', 'data_in', 'data_out',
                                   'validated_jobs', 'validated_payloads',
                                   'finished_jobs', 'finished_payloads', 'finished_data_in', 'finished_data_out',
                                   'failed_jobs', 'failed_payloads', 'failed_data_in', 'failed_data_out'])

    queues.jobs = Queue.Queue()
    queues.payloads = Queue.Queue()
    queues.data_in = Queue.Queue()
    queues.data_out = Queue.Queue()

    queues.validated_jobs = Queue.Queue()
    queues.validated_payloads = Queue.Queue()

    queues.finished_jobs = Queue.Queue()
    queues.finished_payloads = Queue.Queue()
    queues.finished_data_in = Queue.Queue()
    queues.finished_data_out = Queue.Queue()

    queues.failed_jobs = Queue.Queue()
    queues.failed_payloads = Queue.Queue()
    queues.failed_data_in = Queue.Queue()
    queues.failed_data_out = Queue.Queue()

    logger.info('setting up tracing')

    traces = namedtuple('traces', ['pilot',
                                   'rucio'])
    traces.pilot = {'state': SUCCESS,
                    'nr_jobs': 0}
    traces.rucio = {}

    logger.info('starting threads')

    threads = [threading.Thread(target=job.control,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=payload.control,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=data.control,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args})]

    [t.start() for t in threads]

    lifetime.control(traces, args)

    logger.info('waiting for interrupts')

    # Interruptible joins require a timeout
    while threading.activeCount() > 1:
        [t.join(timeout=1) for t in threads]

    return traces
