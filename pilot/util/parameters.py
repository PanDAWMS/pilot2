#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

# This module contains functions that are used with the get_parameters() function defined in the information module.

# WARNING: IN GENERAL, NEEDS TO USE PLUG-IN MANAGER

from pilot.util.information import get_parameter

import logging
logger = logging.getLogger(__name__)


def get_maximum_input_sizes(queuedata):
    """
    This function returns the maximum allowed size for all input files. The sum of all input file sizes should not
    exceed this value.

    :param queuedata: the queuedata dictionary from schedconfig.
    :return: maxinputsizes (integer value in GB).
    """

    try:
        _maxinputsizes = int(get_parameter(queuedata, 'maxwdir'))  # normally 14336+2000 MB
    except TypeError, e:
        from pilot.util.config import config
        _maxinputsizes = config.Pilot.maximum_input_file_sizes  # MB
        logger.warning('could not convert schedconfig value for maxwdir: %s (will use default value instead - %d GB)' %
                       (e, _maxinputsizes))

    return _maxinputsizes
