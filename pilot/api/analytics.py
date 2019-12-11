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
from pilot.util.filehandling import get_table_from_file
from pilot.util.math import mean, sum_square_dev, sum_dev, chi2, float_to_rounded_string

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

    def get_table(self, filename, header=None, separator="\t", convert_to_float=True):
        """

        :param filename: full path to input file (string).
        :param header: header string.
        :param separator: separator character (char).
        :param convert_to_float: boolean, if True, all values will be converted to floats.
        :return: dictionary.
        """

        return get_table_from_file(filename, header=header, separator=separator, convert_to_float=convert_to_float)

    def get_fitted_data(self, filename, x_name='Time', y_name='pss+swap', precision=2, tails=True):
        """
        Return a properly formatted job metrics string with analytics data.
        Currently the function returns a fit for PSS+Swap vs time, whose slope measures memory leaks.

        :param filename: full path to memory monitor output (string).
        :param x_name: optional string, name selector for table column.
        :param y_name: optional string, name selector for table column.
        :param precision: optional precision for fitted slope parameter, default 2.
        :param tails: should tails (first and last values) be used? (boolean).
        :return: {"slope": slope, "chi2": chi2} (float strings with desired precision).
        """

        slope = ""
        chi2 = ""
        table = self.get_table(filename)

        if table:
            # extract data to be fitted
            x, y = self.extract_from_table(table, x_name, y_name)

            # remove tails if desired
            # this is useful e.g. for memory monitor data where the first and last values
            # represent allocation and de-allocation, ie not interesting
            if not tails and len(x) > 7 and len(y) > 7:
                logger.debug('removing tails from data to be fitted')
                x = x[5:]
                x = x[:-2]
                y = y[5:]
                y = y[:-2]

            if len(x) > 7 and len(y) > 7:
                logger.info('fitting %s vs %s' % (y_name, x_name))
                try:
                    fit = self.fit(x, y)
                    _slope = self.slope()
                except Exception as e:
                    logger.warning('failed to fit data, x=%s, y=%s: %s' % (str(x), str(y), e))
                else:
                    if _slope:
                        slope = float_to_rounded_string(fit.slope(), precision=precision)
                        chi2 = float_to_rounded_string(fit.chi2(), precision=0)  # decimals are not needed for chi2
                        if slope != "":
                            logger.info('current memory leak: %s B/s (using %d data points, chi2=%s)' %
                                        (slope, len(x), chi2))
            else:
                logger.warning('wrong length of table data, x=%s, y=%s (must be same and length>=4)' % (str(x), str(y)))

        return {"slope": slope, "chi2": chi2}

    def extract_from_table(self, table, x_name, y_name):
        """

        :param table: dictionary with columns.
        :param x_name: column name to be extracted (string).
        :param y_name: column name to be extracted (may contain '+'-sign) (string).
        :return: x (list), y (list).
        """

        x = table.get(x_name, [])
        if '+' not in y_name:
            y = table.get(y_name, [])
        else:
            try:
                y1_name = y_name.split('+')[0]
                y2_name = y_name.split('+')[1]
                y1_value = table.get(y1_name, [])
                y2_value = table.get(y2_name, [])
            except Exception as e:
                logger.warning('exception caught: %s' % e)
                x = []
                y = []
            else:
                # create new list with added values (1,2,3) + (4,5,6) = (5,7,9)
                y = [x0 + y0 for x0, y0 in zip(y1_value, y2_value)]

        return x, y


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

        # base calculations
        if self._model == 'linear':
            self._ss = sum_square_dev(self._x)
            self._ss2 = sum_dev(self._x, self._y)
            self.set_slope()
            self._xm = mean(self._x)
            self._ym = mean(self._y)
            self.set_intersect()
            self.set_chi2()
        else:
            raise NotImplemented("\'%s\' model is not implemented" % self._model)

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

        y_observed = self._y
        y_expected = []
        #i = 0
        for x in self._x:
            #y_expected.append(self.value(x) - y_observed[i])
            y_expected.append(self.value(x))
            #i += 1
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
