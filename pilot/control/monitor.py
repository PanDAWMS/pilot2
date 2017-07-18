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
logger = logging.getLogger(__name__)


def control(queues, traces, args):

    while not args.graceful_stop.is_set():
        time.sleep(30*60)
        run_checks()
        send_heartbeat()


def run_checks():
    pass


def send_heartbeat():
    pass
