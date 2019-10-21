#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

import os

try:
    import ConfigParser
except Exception:  # python 3
    import configparser as ConfigParser  # noqa: N812

_default_cfg = os.path.join(os.path.dirname(__file__), 'default.cfg')


class _ConfigurationSection(object):
    """
    Keep the settings for a section of the configuration file
    """

    def __getitem__(self, item):
        return getattr(self, item)

    def __repr__(self):
        return str(tuple(self.__dict__.keys()))

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            raise AttributeError('Setting \"%s\" does not exist in the section' % attr)

def read(config_file):
    """
    Read the settings from file and return a dot notation object
    """

    _config = ConfigParser.ConfigParser()
    _config.read(config_file)

    obj = _ConfigurationSection()

    for section in _config.sections():
        
        settings = _ConfigurationSection()
        for key, value in _config.items(section):
            setattr(settings, key, value)

        setattr(obj, section, settings)

    return obj


config = read(_default_cfg)
