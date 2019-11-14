#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

from os import getcwd
from .services import Services

import logging
logger = logging.getLogger(__name__)


class MemoryMonitoring(Services):
    """
    Memory monitoring service class.
    """

    user = ""     # Pilot user, e.g. 'ATLAS'
    pid = 0       # Job process id
    workdir = ""  # Job work directory
    _cmd = ""     # Memory monitoring command (full path, all options)

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        """

        for key in kwargs:
            setattr(self, key, kwargs[key])

        if not self.workdir:
            self.workdir = getcwd()

        if self.user:
            user_utility = __import__('pilot.user.%s.utilities' % self.user, globals(), locals(), [self.user], 0)  # Python 2/3
            self._cmd = user_utility.get_memory_monitor_setup(self.pid, self.workdir)

    def get_command(self):
        """
        Return the full command for the memory monitor.

        :return: command string.
        """

        return self._cmd

    def execute(self):
        """
        Execute the memory monitor command.
        Return the process.

        :return: process.
        """

        return None

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
