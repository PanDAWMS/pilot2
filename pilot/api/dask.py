#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

#from pilot.common.exception import NotDefined, NotSameLength, UnknownException
#from pilot.util.filehandling import get_table_from_file
#from pilot.util.math import mean, sum_square_dev, sum_dev, chi2, float_to_rounded_string

import logging
logger = logging.getLogger(__name__)


class Dask(object):
    """
    Dask interface class.
    """

    status = None
    loadbalancerip = None

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        """

        pass

    def install(self):
        """

        """

        pass
