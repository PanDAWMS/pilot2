#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

import time
import logging
import os
from pilot.util.disk import disk_usage
from pilot.util.config import config, human2bytes

logger = logging.getLogger(__name__)


def control(queues, traces, args):

    while not args.graceful_stop.is_set():
        time.sleep(30 * 60)
        run_checks(args)
        send_heartbeat()


def check_local_space_limit():
    du = disk_usage(os.path.abspath("."))
    return du[2] < human2bytes(config.Python.localspace_limit)


def run_checks(args):
    if not check_local_space_limit():
        return args.graceful_stop.set()


def send_heartbeat():
    pass
