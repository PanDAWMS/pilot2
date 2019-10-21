#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

import os
import re
import collections

try:
    import ConfigParser
except Exception:  # python 3
    import configparser as ConfigParser  # noqa: N812

_section_internal = collections.namedtuple('config', 'conf name')

_default_cfg = os.path.join(os.path.dirname(__file__), 'default.cfg')
_locations = [
    os.path.expanduser('~/.panda/pilot.cfg'),
    '/etc/panda/pilot.cfg',
    './pilot.cfg'
]


class Section(object):
    int = None

    def __init__(self, _config, name):
        object.__setattr__(self, 'int', _section_internal(_config, name))

    def __getitem__(self, item):
        i = object.__getattribute__(self, 'int')
        return i.conf.get(i.name, item, None)

    def __getattr__(self, item):
        i = object.__getattribute__(self, 'int')
        return i.conf.get(i.name, item, None)

    def __contains__(self, key):
        i = object.__getattribute__(self, 'int')
        return i.conf.has_option(i.name, key)

    def __dir__(self):
        i = object.__getattribute__(self, 'int')
        return i.conf.options(i.name)

    def __iter__(self):
        i = object.__getattribute__(self, 'int')
        return i.conf.options(i.name).__iter__()

    def __setitem__(self, key, value):
        i = object.__getattribute__(self, 'int')
        i.conf.set(i.conf.name, key, value)

    def __setattr__(self, key, value):
        i = object.__getattribute__(self, 'int')
        i.conf.set(i.conf.name, key, value)

    def __delattr__(self, item):
        i = object.__getattribute__(self, 'int')
        if i.conf.has_option(i.name, item):
            i.conf.remove_option(i.name, item)

    def __delitem__(self, item):
        i = object.__getattribute__(self, 'int')
        if i.conf.has_option(i.name, item):
            i.conf.remove_option(i.name, item)

    def get(self, *arg):
        i = object.__getattribute__(self, 'int')
        return i.conf.get(i.conf.name, *arg)


class ExtendedConfig(ConfigParser.ConfigParser):
    def __init__(self):
        ConfigParser.ConfigParser.__init__(self)

    def __getitem__(self, item):
        if self.has_section(item):
            return Section(self, item)
        raise ConfigParser.NoSectionError(item)

    def __getattr__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            if self.has_section(item):
                return Section(self, item)
            raise ConfigParser.NoSectionError(item)

    def __iter__(self):
        return self.sections().__iter__()

    def __dir__(self):
        def has_attr(obj, attr):
            """ Wrapper for hasattr() to resolve python 2 vs 3 issue """
            # See https://medium.com/@k.wahome/python-2-vs-3-hasattr-behaviour-f1bed48b068
            has_the_attr = False
            try:
                has_the_attr = hasattr(obj, attr)
            except Exception:  # python 3 will raise an exception rather than returning False
                pass
            return has_the_attr

        def get_attrs(obj):
            import types
            if not has_attr(obj, '__dict__'):
                return []  # slots only
            arg = (dict, types.DictProxyType) if has_attr(types, 'DictProxyType') else dict  # python 3 correction
            if not isinstance(obj.__dict__, arg):
                raise TypeError("%s.__dict__ is not a dictionary" % obj.__name__)
            return obj.__dict__.keys()

        def dir2(obj):
            _dir = set()
            if not has_attr(obj, '__bases__'):
                # obj is an instance
                if not has_attr(obj, '__class__'):
                    # slots
                    return sorted(get_attrs(obj))
                _class = obj.__class__
                _dir.update(get_attrs(_class))
            else:
                # obj is a class
                _class = obj

            for cls in _class.__bases__:
                _dir.update(get_attrs(cls))
                _dir.update(dir2(cls))
            _dir.update(get_attrs(obj))
            return _dir

        _dir = set(dir2(self))
        _dir.update(self.sections())

        return list(_dir)

    def __contains__(self, key):
        return self.has_section(key)

    def __delattr__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            if self.has_section(item):
                self.remove_section(item)
            else:
                object.__delattr__(self, item)

    def __delitem__(self, item):
        if self.has_section(item):
            self.remove_section(item)


config = ExtendedConfig()
config.readfp(open(_default_cfg))
config.read(_locations)
