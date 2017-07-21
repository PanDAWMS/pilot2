#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017


import ConfigParser
import os
import re

_default_cfg = os.path.join(os.path.dirname(__file__), 'default.cfg')
_locations = [
    os.path.expanduser('~/.panda/pilot.cfg'),
    '/etc/panda/pilot.cfg',
    './pilot.cfg'
]


class Section(object):
    config = None
    name = None

    def __init__(self, _config, name):
        object.__setattr__(self, 'config', _config)
        object.__setattr__(self, 'name', name)

    def __getitem__(self, item):
        return object.__getattribute__(self, 'config').get(object.__getattribute__(self, 'name'), item, None)

    def __getattr__(self, item):
        return object.__getattribute__(self, 'config').get(object.__getattribute__(self, 'name'), item, None)

    def __setitem__(self, key, value):
        object.__getattribute__(self, 'config').set(object.__getattribute__(self, 'name'), key, value)

    def __setattr__(self, key, value):
        object.__getattribute__(self, 'config').set(object.__getattribute__(self, 'name'), key, value)

    def get(self, *arg):
        return object.__getattribute__(self, 'config').get(object.__getattribute__(self, 'name'), *arg)


class ExtendedConfig(ConfigParser.ConfigParser):
    def __init__(self):
        ConfigParser.ConfigParser.__init__(self)

    def __getitem__(self, item):
        if self.has_section(item):
            return Section(self, item)
        return None

    def __getattr__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            if self.has_section(item):
                return Section(self, item)
            raise

SYMBOLS = {
    'customary': ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext': ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),

    'iec': ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext': ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi', 'zebi', 'yobi'),
}


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
    letter = re.sub(r'(?i)(?<=.)(bi?|bytes?)$', "", letter)
    if len(letter) == 0:
        letter = "B"

    for name, sset in SYMBOLS.items():
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


config = ExtendedConfig()
config.readfp(open(_default_cfg))
config.read(_locations)
