#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# from pilot.util.auxiliary import get_logger
from pilot.common.exception import NotDefined

from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


def mean(data):
    """
    Return the sample arithmetic mean of data.

    :param data: list of floats or ints.
    :return: mean value (float).
    """

    n = len(data)
    if n < 1:
        raise ValueError('mean requires at least one data point')

    # return sum(data)/n # in Python 2 use sum(data)/float(n)
    return sum(data) / float(n)


def sum_square_dev(data):
    """
    Return sum of square deviations of sequence data.
    Sum (x - x_mean)**2

    :param data: list of floats or ints.
    :return: sum of squares (float).
    """

    c = mean(data)

    return sum((x - c) ** 2 for x in data)


def sum_dev(x, y):
    """
    Return sum of deviations of sequence data.
    Sum (x - x_mean)**(y - y_mean)

    :param x: list of ints or floats.
    :param y:  list of ints or floats.
    :return: sum of deviations (float).
    """

    c1 = mean(x)
    c2 = mean(y)

    return sum((_x - c1) * (_y - c2) for _x, _y in zip(x, y))


def chi2(observed, expected):
    """
    Return the chi2 sum of the provided observed and expected values.

    :param observed: list of floats.
    :param expected: list of floats.
    :return: chi2 (float).
    """

    if 0 in expected:
        return 0.0

    return sum((_o - _e) ** 2 / _e for _o, _e in zip(observed, expected))


def float_to_rounded_string(num, precision=3):
    """
    Convert float to a string with a desired number of digits (the precision).
    E.g. num=3.1415, precision=2 -> '3.14'.

    :param num: number to be converted (float).
    :param precision: number of desired digits (int)
    :raises NotDefined: for undefined precisions and float conversions to Decimal.
    :return: rounded string.
    """

    try:
        _precision = Decimal(10) ** -precision
    except Exception as e:
        raise NotDefined('failed to define precision=%s: %e' % (precision, e))

    try:
        s = Decimal(str(num)).quantize(_precision)
    except Exception as e:
        raise NotDefined('failed to convert %s to Decimal: %s' % (str(num), e))

    return str(s)
