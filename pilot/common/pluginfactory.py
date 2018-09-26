#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018


import logging
logger = logging.getLogger(__name__)

"""
A factory to manage plugins
"""


class PluginFactory(object):

    def __init__(self, *args, **kwargs):
        self.classMap = {}

    def get_plugin(self, confs):
        """
        Load plugin class

        :param confs: a dict of configurations.
        """

        class_name = confs['class']
        if class_name is None:
            logger.error("[class] is not defined in confs: %s" % confs)
            return None

        if class_name not in self.classMap:
            logger.info("Trying to import %s" % class_name)
            components = class_name.split('.')
            mod = __import__('.'.join(components[:-1]))
            for comp in components[1:]:
                mod = getattr(mod, comp)
            self.classMap[class_name] = mod

        args = {}
        for key in confs:
            if key in ['class']:
                continue
            args[key] = confs[key]

        cls = self.classMap[class_name]
        logger.info("Importing %s with args: %s" % (cls, args))
        impl = cls(**args)

        return impl
