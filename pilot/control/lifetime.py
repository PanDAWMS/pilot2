#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017

import time
from pilot.util import signalling
from pilot.util.async_decorator import async

import logging
logger = logging.getLogger(__name__)

_start = None
_max = None


def log_lifetime(sig, frame):
    logger.info('lifetime: {0}s used, {1}s maximum'.format(int(time.time()-_start), _max))


@async(daemon=True)
def control(traces, args):
    global _max, _start
    _max = args.lifetime
    _start = time.time()
    signalling.signal_all_setup(log_lifetime)

    if _max is not None and _max > 0:
        time.sleep(_max)

        logger.debug('maximum lifetime reached: {0}s'.format(_max))
        signalling.simulate_signal()
