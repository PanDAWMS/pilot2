#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

from .services import Services

import logging
logger = logging.getLogger(__name__)


class MemoryMonitoring(Services):
    """
    Memory monitoring service class.
    """

    user = ""  # Pilot user, e.g. 'ATLAS'
    _cmd = ""  # Memory monitoring command (full path, all options)

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        """

        for key in kwargs:
            setattr(self, key, kwargs[key])

    def get_command(self):
        """
        Return the full command for the memory monitor.

        :return: command string.
        """

        return self._cmd

    def get_filename(self):
        """
        ..

        :return:
        """

        return ""

    def get_results(self):
        """
        ..

        :return:
        """

        return None
