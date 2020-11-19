#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

# from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


class Services(object):
    """
    High-level base class for Benchmark(), MemoryMonitoring() and Analytics() classes.
    """

    def __init__(self, *args):
        """
        Init function.

        :param args:
        """

        pass
