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


class Analytics(Services):
    """
    Analytics service class.
    """

    _fit = None

    def __init__(self, *args):
        """
        Init function.

        :param args:
        """

        self._fit = None

        pass

    def fit(self, x, y, model='linear'):
        """
        Fitting function.

        :param x: list of input data (list of floats or ints).
        :param y: list of input data (list of floats or ints).
        :param model: model name (string).
        :return:
        """

        self._fit = Fit()

        return self._fit

    def chi2(self):
        """
        Return the Chi2 of the fit.

        :return: chi2 (float).
        """

        # calculate Chi2
        # ..
        return 0.0

    def slope(self):
        """
        Calculate the slope of a linear fit.

        :return: slope (float).
        """

        # calculate slope
        # ..
        return 0.0

    def intersect(self):
        """
        Calculate the intersect of a linear fit.

        :return: intersect (float).
        """

        # calculate slope
        # ..
        return 0.0


class Fit(object):
    """
    Low-level fitting class.
    """

    def __init__(self, *args):
        """
        Init function.

        :param args:
        """

        pass
