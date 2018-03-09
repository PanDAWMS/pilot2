#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import functools
import signal
from collections import namedtuple

# from pilot.control import job, payload, data, lifetime, monitor
from pilot.util.constants import SUCCESS, FAILURE

import logging
logger = logging.getLogger(__name__)


def interrupt(args, signum, frame):
    logger.info('caught signal: %s' % [v for v, k in signal.__dict__.iteritems() if k == signum][0])
    args.graceful_stop.set()


def run(args):
    """
     Main execution function for the generic HPC workflow.

     :param args: pilot arguments.
     :returns: traces.
     """

    logger.info('setting up signal handling')
    signal.signal(signal.SIGINT, functools.partial(interrupt, args))

    logger.info('setting up tracing')
    traces = namedtuple('traces', ['pilot'])
    traces.pilot = {'state': SUCCESS,
                    'nr_jobs': 0}

    if args.hpc_resource == '':
        logger.critical('hpc resource not specified, cannot continue')
        traces.pilot['state'] = FAILURE
        return traces

    logger.info('hpc resource: %s' % args.hpc_resource)

    # implement main function here
    # ..

    return traces
