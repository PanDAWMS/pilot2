#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

# NOTE: this component should be tied to the job, its thread should live and die with the job

import logging
import os
from pilot.util.disk import disk_usage
from pilot.util.config import config, human2bytes

logger = logging.getLogger(__name__)


def control(queues, traces, args):
    while not args.graceful_stop.is_set():
        if args.graceful_stop.wait(30 * 60) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility reasons
            break
        run_checks(args)
        send_heartbeat()


def check_local_space_limit():
    du = disk_usage(os.path.abspath("."))
    return du[2] < human2bytes(config.Pilot.free_space_limit)


def check_output_file_sizes():
    return True  # Needs to get somehow job parameters.


def run_checks(args):
    if not check_local_space_limit():
        return args.graceful_stop.set()

    if not check_output_file_sizes():
        return args.graceful_stop.set()


def send_heartbeat():
    pass
