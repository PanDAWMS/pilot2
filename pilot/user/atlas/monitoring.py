#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

import logging
logger = logging.getLogger(__name__)


def fast_monitor_tasks(job):
    """
    Perform fast monitoring tasks.

    :param job: job object.
    :return: exit code (int)
    """

    exit_code = 0

    logger.debug('fast monitor called')

    return exit_code
