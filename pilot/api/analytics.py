#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from .services import Services
from pilot.common.exception import NotImplemented, NotDefined, NotSameLength, UnknownException
from pilot.util.math import mean, sum_square_dev, sum_dev, chi2

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
        For a linear model: y(x) = slope * x + intersect

        :param x: list of input data (list of floats or ints).
        :param y: list of input data (list of floats or ints).
        :param model: model name (string).
        :raises UnknownException: in case Fit() fails.
        :return:
        """

        try:
            self._fit = Fit(x=x, y=y, model=model)
        except Exception as e:
            raise UnknownException(e)

        return self._fit

    def slope(self):
        """
        Return the slope of a linear fit, y(x) = slope * x + intersect.

        :raises NotDefined: exception thrown if fit is not defined.
        :return: slope (float).
        """

        slope = None

        if self._fit:
            slope = self._fit.slope()
        else:
            raise NotDefined('Fit has not been defined')

        return slope

    def intersect(self):
        """
        Return the intersect of a linear fit, y(x) = slope * x + intersect.

        :raises NotDefined: exception thrown if fit is not defined.
        :return: intersect (float).
        """

        intersect = None

        if self._fit:
            intersect = self._fit.intersect()
        else:
            raise NotDefined('Fit has not been defined')

        return intersect

    def chi2(self):
        """
        Return the chi2 of the fit.

        :raises NotDefined: exception thrown if fit is not defined.
        :return: chi2 (float).
        """

        x2 = None

        if self._fit:
            x2 = self._fit.chi2()
        else:
            raise NotDefined('Fit has not been defined')

        return x2


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
    _chi2 = None  # chi2

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        :raises PilotException: NotImplemented for unknown fitting model, NotDefined if input data not defined.
        """

        # extract parameters
        self._model = kwargs.get('model', 'linear')
        self._x = kwargs.get('x', None)
        self._y = kwargs.get('y', None)

        if not self._x or not self._y:
            raise NotDefined('input data not defined')

        if len(self._x) != len(self._y):
            raise NotSameLength('input data (lists) have different lengths')

        print 'x=', self._x
        print 'y=', self._y
        # base calculations
        if self._model == 'linear':
            self._ss = sum_square_dev(self._x)
            print 'ss=', self._ss

            self._ss2 = sum_dev(self._x, self._y)
            print 'ss2=', self._ss2

            self.set_slope()
            print 'slope=', self.slope()

            self._xm = mean(self._x)
            print 'xm=', self._xm

            self._ym = mean(self._y)
            print 'ym=', self._ym
            self.set_intersect()
            print 'intersect=', self.intersect()

            self.set_chi2()
            print 'chi2=', self.chi2()
        else:
            raise NotImplemented("\'%s\' model is not implemented" % self._model)

        print 'OK'

    def fit(self):
        """
        Return fitting object.

        :return: fitting object.
        """

        return self

    def value(self, t):
        """
        Return the value y(x=t) of a linear fit y(x) = slope * x + intersect.

        :return: intersect (float).
        """

        return self._slope * t + self._intersect

    def set_chi2(self):
        """
        Calculate and set the chi2 value.

        :return:
        """

        _chi2 = None

        y_observed = self._y
        y_expected = []
        i = 0
        for x in self._x:
            y_expected.append(self.value(x) - y_observed[i])
            i += 1
        if y_observed and y_observed != [] and y_expected and y_expected != []:
            self._chi2 = chi2(y_observed, y_expected)
        else:
            self._chi2 = None

    def chi2(self):
        """
        Return the chi2 value.

        :return: chi2 (float).
        """

        return self._chi2

    def set_slope(self):
        """
        Calculate and set the slope of the linear fit.

        :return:
        """

        if self._ss2 and self._ss and self._ss != 0:
            self._slope = self._ss2 / float(self._ss)
        else:
            self._slope = None

    def slope(self):
        """
        Return the slope value.

        :return: slope (float).
        """

        return self._slope

    def set_intersect(self):
        """
        Calculate and set the intersect of the linear fit.

        :return:
        """

        if self._ym and self._slope and self._xm:
            self._intersect = self._ym - self._slope * self._xm
        else:
            self._intersect = None

    def intersect(self):
        """
        Return the intersect value.

        :return: intersect (float).
        """

        return self._intersect
