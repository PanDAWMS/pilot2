#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2020

from pilot.common.exception import NotDefined

from decimal import Decimal
from re import split, sub

import logging
logger = logging.getLogger(__name__)

SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),

    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'),
}


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


def diff_lists(list_a, list_b):
    """
    Return the difference between list_a and list_b.

    :param list_a: input list a.
    :param list_b: input list b.
    :return: difference (list).
    """

    return list(set(list_a) - set(list_b))


def bytes2human(n, _format='%(value).1f %(symbol)s', symbols='customary'):
    """
    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec" or "iec_ext",
    see: http://goo.gl/kTQMs

      >>> bytes2human(0)
      '0.0 B'
      >>> bytes2human(0.9)
      '0.0 B'
      >>> bytes2human(1)
      '1.0 B'
      >>> bytes2human(1.9)
      '1.0 B'
      >>> bytes2human(1024)
      '1.0 K'
      >>> bytes2human(1048576)
      '1.0 M'
      >>> bytes2human(1099511627776127398123789121)
      '909.5 Y'

      >>> bytes2human(9856, symbols="customary")
      '9.6 K'
      >>> bytes2human(9856, symbols="customary_ext")
      '9.6 kilo'
      >>> bytes2human(9856, symbols="iec")
      '9.6 Ki'
      >>> bytes2human(9856, symbols="iec_ext")
      '9.6 kibi'

      >>> bytes2human(10000, "%(value).1f %(symbol)s/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> bytes2human(10000, _format="%(value).5f %(symbol)s")
      '9.76562 K'
    """
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")
    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i + 1) * 10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return _format % locals()
    return _format % dict(symbol=symbols[0], value=n)


def human2bytes(s, divider=None):
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    When unable to recognize the format ValueError is raised.

    If no digit passed, only a letter, it is interpreted as a one of a kind. Eg "KB" = "1 KB".
    If no letter passed, it is assumed to be in bytes. Eg "512" = "512 B"

    The second argument is used to convert to another magnitude (eg return not bytes but KB).
    It can be interpreted as a cluster size. Eg "512 B", or "0.2 K".

      >>> human2bytes('0 B')
      0
      >>> human2bytes('3')
      3
      >>> human2bytes('K')
      1024
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1 M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776

      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'

      >>> human2bytes('1 M', 'K')
      1024
      >>> human2bytes('2 G', 'M')
      2048
      >>> human2bytes('G', '2M')
      512
    """
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]

    if len(num) == 0:
        num = "1"
    num = float(num)
    letter = s.strip()
    letter = sub(r'(?i)(?<=.)(bi?|bytes?)$', "", letter)
    if len(letter) == 0:
        letter = "B"

    for name, sset in list(SYMBOLS.items()):
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]: 1}
    for i, s in enumerate(sset[1:]):
        prefix[s] = 1 << (i + 1) * 10

    div = 1 if divider is None else human2bytes(divider)
    return int(num * prefix[letter] / div)
