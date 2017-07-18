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

_default_cfg = os.path.join(os.path.dirname(__file__), 'default.cfg')
_locations = [
    os.path.expanduser('~/.panda/pilot.cfg'),
    '/etc/panda/pilot.cfg',
    './pilot.cfg'
]

config = ConfigParser.ConfigParser()
config.readfp(open(_default_cfg))
config.read(_locations)
