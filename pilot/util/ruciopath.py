#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018

import hashlib


def get_rucio_path(scope, name):
    """
    Construct Rucio standard path using the scope and the LFN
    """

    s = '%s:%s' % (scope, name)
    hash_hex = hashlib.md5(s.encode('utf-8')).hexdigest()  # Pythno 2/3

    paths = scope.split('.') + [hash_hex[0:2], hash_hex[2:4], name]

    return '/'.join(paths)
