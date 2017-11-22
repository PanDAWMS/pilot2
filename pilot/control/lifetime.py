#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017

import time

import logging
logger = logging.getLogger(__name__)

# NOTE: rename this component and add internal thread monitoring, keep global lifetime monitoring


def log_lifetime(sig, frame, traces):
    logger.info('lifetime: %i used, %i maximum' % (int(time.time() - traces.pilot['lifetime_start']),
                                                   traces.pilot['lifetime_max']))


def control(queues, traces, args):

    traces.pilot['lifetime_start'] = time.time()
    traces.pilot['lifetime_max'] = time.time()

    runtime = 0
    while not args.graceful_stop.is_set():
        if runtime < args.lifetime:
            time.sleep(1)
            runtime += 1
        else:
            logger.debug('maximum lifetime reached: %s' % args.lifetime)
            args.graceful_stop.set()

    logger.info('lifetime: %s used, %s maximum' % (runtime, args.lifetime))
