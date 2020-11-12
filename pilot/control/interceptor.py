#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

import time

try:
    import Queue as queue  # noqa: N813
except Exception:
    import queue  # Python 3

from pilot.common.exception import ExcThread
from pilot.util.processes import threads_aborted

import logging
logger = logging.getLogger(__name__)


def run(args):
    """
    Main execution function for the interceptor communication layer.

    :param args: pilot arguments.
    :returns:
    """

    # t = threading.current_thread()
    # logger.debug('job.control is run by thread: %s' % t.name)

    targets = {'receive': receive, 'send': send}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'args': args},
                         name=name) for name, target in list(targets.items())]  # Python 2/3

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

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[interceptor] run thread has finished')


def receive(args):
    """
    Look for interceptor messages.

    :param args: Pilot args object.
    :return:
    """

    while not args.graceful_stop.is_set():
        time.sleep(0.5)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[interceptor] receive thread has finished')


def send(args):
    """
    Send message to interceptor.

    :param args: Pilot args object.
    :return:
    """

    while not args.graceful_stop.is_set():
        time.sleep(0.5)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[interceptor] receive send has finished')
