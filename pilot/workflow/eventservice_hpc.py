#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

import functools
import signal
from collections import namedtuple
from os import environ

from pilot.util.constants import SUCCESS, FAILURE

import logging
logger = logging.getLogger(__name__)


def interrupt(args, signum, frame):
    try:
        logger.info('caught signal: %s' % [v for v, k in signal.__dict__.iteritems() if k == signum][0])
    except Exception:
        logger.info('caught signal: %s' % [v for v, k in list(signal.__dict__.items()) if k == signum][0])
    args.graceful_stop.set()


def run(args):
    """
    Main execution function for the event service workflow on HPCs (Yoda-Droid).

    :param args: pilot arguments.
    :returns: traces object.
    """

    try:
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

        # get the resource reference
        resource = __import__('pilot.resource.%s' % args.hpc_resource, globals(), locals(), [args.hpc_resource], 0)  # Python 2/3

        # example usage:
        logger.info('setup for resource %s: %s' % (args.hpc_resource, str(resource.get_setup())))

        # are we Yoda or Droid?
        if environ.get('SOME_ENV_VARIABLE', '') == 'YODA':
            yodadroid = __import__('pilot.eventservice.yoda')
        else:
            yodadroid = __import__('pilot.eventservice.droid')
        yodadroid.run()

    except Exception as e:
        logger.fatal('exception caught: %s' % e)

    return traces
