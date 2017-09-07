#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

# This module contains functions that are used with the get_parameters() function defined in the information module.

# WARNING: IN GENERAL, NEEDS TO USE PLUG-IN MANAGER

from pilot.util.information import get_parameter

import logging
logger = logging.getLogger(__name__)


def get_maximum_input_size():
    """
    This function returns the maximum allowed size for all input files. The sum of all input file sizes should not
    exceed this value.

    :return:
    """

    _maxinputsize = get_parameter('maxwdir')  # normally 14336+2000 MB

    return 0

