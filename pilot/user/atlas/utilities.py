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

import logging
logger = logging.getLogger(__name__)


def get_memory_monitor_setup(job):
    """
    Return the proper setup for the memory monitor.
    If the payload release is provided, the memory monitor can be setup with the same release. Until early 2018, the
    memory monitor was still located in the release area. After many problems with the memory monitor, it was decided
    to use a fixed version for the setup. Currently, release 21.0.22 is used.

    :param job: job object.
    :return: memory monitor setup string.
    """

    return '<some setup>'


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
