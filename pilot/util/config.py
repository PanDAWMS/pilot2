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

config = ExtendedConfig()
config.readfp(open(_default_cfg))
config.read(_locations)
