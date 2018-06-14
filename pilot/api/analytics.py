#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from .services import Services
from pilot.common.exception import NotImplemented, NotDefined, NotSameLength
from pilot.util.math import mean, sum_square_dev, sum_dev

import logging
logger = logging.getLogger(__name__)


class Analytics(Services):
    """
    Analytics service class.
    """

    _fit = None

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
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

        try:
            self._fit = Fit(x=x, y=y, model=model)
        except Exception as e:
            self._fit = None
            raise e

        return self._fit

    def chi2(self):
        """
        Return the Chi2 of the fit.

        :raises NotDefined: exception thrown if fit is not defined.
        :return: chi2 (float).
        """

        chi2 = None

        raise NotImplemented('Chi2 function has not been implemented yet')

        # calculate Chi2
        # ..
        if self._fit:
            pass
        else:
            raise NotDefined('Fit has not been defined')

        return chi2

    def slope(self):
        """
        Return the slope of a linear fit.

        :raises NotDefined: exception thrown if fit is not defined.
        :return: slope (float).
        """

        slope = None

        if self._fit:
            slope = self._fit.slope
        else:
            raise NotDefined('Fit has not been defined')

        return slope

    def intersect(self):
        """
        Return the intersect of a linear fit.

        :raises NotDefined: exception thrown if fit is not defined.
        :return: intersect (float).
        """

        intersect = None

        if self._fit:
            intersect = self._fit.intersect
        else:
            raise NotDefined('Fit has not been defined')

        return intersect


class Fit(object):
    """
    Low-level fitting class.
    """

    _model = 'linear'  # fitting model
    _x = None  # x values
    _y = None  # y values
    _xm = None  # x mean
    _ym = None  # y mean
    _ss = None  # sum of square deviations
    _ss2 = None  # sum of deviations
    _slope = None  # slope
    _intersect = None  # intersect

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        :raises PilotException: NotImplemented for unknown fitting model
        """

        # extract parameters
        self._model = kwargs.get('model', 'linear')
        self._x = kwargs.get('x', None)
        self._y = kwargs.get('y', None)

        if len(self._x) != len(self._y):
            raise NotSameLength('input data (lists) have different lengths')

        # base calculations
        if self._model == 'linear':
            self._ss = sum_square_dev(self._x)
            self._ss2 = sum_dev(self._x, self._y)

            self._slope = self.slope()
            self._xm = mean(self._x)
            self._ym = mean(self._y)
            self._intersect = self.intersect()
        else:
            raise NotImplemented("\'%s\' model is not implemented" % self._model)

    def fit(self):
        """
        Return fitting object.

        :return: fitting object.
        """

        return self

    def slope(self):
        """
        Calculate the slope of the linear fit.

        :return: slope (float).
        """

        if self._ss2 and self._ss and self._ss != 0:
            slope = self._ss2 / float(self._ss)
        else:
            slope = None

        return slope

    def intersect(self):
        """
        Calculate the intersect of the linear fit.

        :return: intersect (float).
        """

        if self._ym and self._slope and self._xm:
            intersect = self._ym - self._slope * self._xm
        else:
            intersect = None

        return intersect
