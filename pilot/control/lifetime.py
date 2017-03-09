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


def control(graceful_stop, traces, args):

    runtime = 0
    while not graceful_stop.is_set():
        if runtime < args.lifetime:
            time.sleep(1)
            runtime += 1
        else:
            logger.debug('maximum lifetime reached: {0}s'.format(args.lifetime))
            graceful_stop.set()

    logger.info('lifetime: {0}s used, {1}s maximum'.format(runtime, args.lifetime))
