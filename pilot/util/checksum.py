#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018

import zlib


def adler32(file):
    """
    Calculate adler32 checksum of a file.

    :returns: Hexified string, padded to 8 values.
    """

    # adler starting value is _not_ 0
    adler = 1L

    try:
        openFile = open(file, 'rb')
        for line in openFile:
            adler = zlib.adler32(line, adler)
    except Exception, e:
        raise Exception('FATAL - could not get checksum of file %s: %s' % (file, e))

    # backflip on 32bit
    if adler < 0:
        adler = adler + 2 ** 32

    return str('%08x' % adler)
