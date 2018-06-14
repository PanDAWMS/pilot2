#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# from pilot.util.auxiliary import get_logger

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
