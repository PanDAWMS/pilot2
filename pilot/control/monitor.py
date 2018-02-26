#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

# NOTE: this module should deal with non-job related monitoring, such as thread monitoring. Job monitoring should
#       be the task of the job_monitor thread in the Job component. Job related functions should be moved to the
#       Job component, with the exception of the heartbeat function.

import Queue
import logging
import os
from pilot.util.disk import disk_usage
from pilot.util.config import config, human2bytes

logger = logging.getLogger(__name__)


# Monitoring of threads functions

def control(queues, traces, args):
    """
    Main control function, run from the relevant workflow module.

    :param queues:
    :param traces:
    :param args:
    :return:
    """
    while not args.graceful_stop.is_set():
        # every 30 ninutes, run the monitoring checks
        if args.graceful_stop.wait(30 * 60) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility reasons
            break

        # proceed with running the checks
        run_checks(args)

        # extract a job from the jobs queue and send it to the heartbeat function
        try:
            job = queues.jobs.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        else:
            send_heartbeat(job)


def run_checks(args):
    if not check_local_space_limit():
        return args.graceful_stop.set()

    if not check_output_file_sizes():
        return args.graceful_stop.set()


def send_heartbeat(job):
    pass


def check_local_space_limit():  # move to Job component?
    du = disk_usage(os.path.abspath("."))
    return du[2] < human2bytes(config.Pilot.free_space_limit)


def check_output_file_sizes():  # move to Job component
    return True
