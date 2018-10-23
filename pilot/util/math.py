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

import sys
from decimal import Decimal
from re import split
from numbers import Number
from collections import Set, Mapping, deque

try: # Python 2
    zero_depth_bases = (basestring, Number, xrange, bytearray)
    iteritems = 'iteritems'
except NameError: # Python 3
    zero_depth_bases = (str, bytes, Number, range, bytearray)
    iteritems = 'items'

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
        raise NotDefined('failed to define precision=%s: %e' % (str(precision), e))

    try:
        s = Decimal(str(num)).quantize(_precision)
    except Exception as e:
        raise NotDefined('failed to convert %s to Decimal: %s' % (str(num), e))

    return str(s)


def tryint(x):
    """
    Used by numbered string comparison (to protect against unexpected letters in version number).

    :param x: possible int.
    :return: converted int or original value in case of ValueError.
    """

    try:
        return int(x)
    except ValueError:
        return x


def split_version(s):
    """
    Split version string into parts and convert the parts into integers when possible.
    Any encountered strings are left as they are.
    The function is used with release strings.
    split_version("1.2.3") = (1,2,3)
    split_version("1.2.Nightly") = (1,2,"Nightly")

    The function can also be used for sorting:
    > names = ['YT4.11', '4.3', 'YT4.2', '4.10', 'PT2.19', 'PT2.9']
    > sorted(names, key=splittedname)
    ['4.3', '4.10', 'PT2.9', 'PT2.19', 'YT4.2', 'YT4.11']

    :param s: release string.
    :return: converted release tuple.
    """

    return tuple(tryint(x) for x in split('([^.]+)', s))


def is_greater_or_equal(a, b):
    """
    Is the numbered string a >= b?
    "1.2.3" > "1.2"  -- more digits
    "1.2.3" > "1.2.2"  -- rank based comparison
    "1.3.2" > "1.2.3"  -- rank based comparison
    "1.2.N" > "1.2.2"  -- nightlies checker, always greater

    :param a: numbered string.
    :param b: numbered string.
    :return: boolean.
    """

    return split_version(a) >= split_version(b)


def add_lists(list1, list2):
    """
    Add list1 and list2 and remove any duplicates.
    Example:
    list1=[1,2,3,4]
    list2=[3,4,5,6]
    add_lists(list1, list2) = [1, 2, 3, 4, 5, 6]

    :param list1: input list 1
    :param list2: input list 2
    :return: added lists with removed duplicates
    """
    return list1 + list(set(list2) - set(list1))


def convert_mb_to_b(size):
    """
    Convert value from MB to B for the given size variable.
    If the size is a float, the function will convert it to int.

    :param size: size in MB (float or int).
    :return: size in B (int).
    :raises: ValueError for conversion error.
    """

    try:
        size = int(size)
    except Exception as e:
        raise ValueError('cannot convert %s to int: %s' % (str(size), e))

    return size * 1024 ** 2


def get_size(obj_0):
    """
    Recursively iterate to sum size of object & members.
    Note: for size measurement to work, the object must have set the data members in the __init__().

    :param obj_0: object to be measured.
    :return: size in Bytes (int).
    """

    _seen_ids = set()

    def inner(obj):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0

        _seen_ids.add(obj_id)
        size = sys.getsizeof(obj)

        if isinstance(obj, zero_depth_bases):
            pass # bypass remaining control flow and return
        elif isinstance(obj, (tuple, list, Set, deque)):
            size += sum(inner(i) for i in obj)
        elif isinstance(obj, Mapping) or hasattr(obj, iteritems):
            size += sum(inner(k) + inner(v) for k, v in getattr(obj, iteritems)())

        # Check for custom object instances - may subclass above too
        if hasattr(obj, '__dict__'):
            size += inner(vars(obj))
        if hasattr(obj, '__slots__'): # can have __slots__ with __dict__
            size += sum(inner(getattr(obj, s)) for s in obj.__slots__ if hasattr(obj, s))

        return size

    return inner(obj_0)

