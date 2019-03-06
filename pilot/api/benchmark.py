#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from .services import Services

import logging
logger = logging.getLogger(__name__)


class Benchmark(Services):
    """
    Benchmark service class.
    """

    def __init__(self, *args):
        """
        Init function.

        :param args:
        """

        pass
