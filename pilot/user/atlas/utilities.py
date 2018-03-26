#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# import os

# from pilot.info import infosys
from pilot.user.atlas.setup import get_asetup

import logging
logger = logging.getLogger(__name__)


def get_memory_monitor_summary_filename():
    """
    Return the name for the memory monitor summary file.

    :return: File name (string).
    """

    return "memory_monitor_summary.json"


def get_memory_monitor_output_filename():
    """
    Return the filename of the memory monitor text output file.

    :return: File name (string).
    """

    return "memory_monitor_output.txt"


def get_memory_monitor_setup(job):
    """
    Return the proper setup for the memory monitor.
    If the payload release is provided, the memory monitor can be setup with the same release. Until early 2018, the
    memory monitor was still located in the release area. After many problems with the memory monitor, it was decided
    to use a fixed version for the setup. Currently, release 21.0.22 is used.

    :param job: job object.
    :return: memory monitor setup string.
    """

    release = "21.0.22"
    platform = "x86_64-slc6-gcc62-opt"
    setup = get_asetup(asetup=prepareasetup) + " Athena," + release + " --platform " + platform
    interval = 60

    # Now add the MemoryMonitor command
    cmd = "%s; MemoryMonitor --pid %d --filename %s --json-summary %s --interval %d" % \
           (setup, job.pid, get_memory_monitor_output_filename, get_memory_monitor_summary_filename(), interval)
    cmd = "cd " + job.workdir + ";" + cmd

    return cmd


def get_network_monitor_setup(setup, job):
    """
    Return the proper setup for the network monitor.
    The network monitor is currently setup together with the payload and is start before it. The payload setup should
    therefore be provided. The network monitor setup is prepended to it.

    :param setup: payload setup string.
    :param job: job object.
    :return: network monitor setup string.
    """

    return ''
