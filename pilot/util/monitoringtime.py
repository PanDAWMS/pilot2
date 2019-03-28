#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import time


class MonitoringTime(object):
    """
    A simple class to store the various monitoring task times.
    Different monitoring tasks should be executed at different intervals. An object of this class is used to store
    the time when a specific monitoring task was last executed. The actual time interval for a given monitoring tasks
    is stored in the util/default.cfg file.
    """

    def __init__(self):
        """
        Return the initial MonitoringTime object with the current time as start values.
        """

        ct = int(time.time())
        self.ct_proxy = ct
        self.ct_looping = ct
        self.ct_looping_last_touched = None
        self.ct_diskspace = ct
        self.ct_memory = ct
        self.ct_process = ct
        self.ct_heartbeat = ct
        # add more here

    def update(self, key, modtime=None):
        """
        Update a given key with the current time or given time.
        Usage: mt=MonitoringTime()
               mt.update('ct_proxy')

        :param key: name of key (string).
        :param modtime: modification time (int).
        :return:
        """

        ct = int(time.time()) if not modtime else modtime
        if hasattr(self, key):
            setattr(self, key, ct)

    def get(self, key):
        """
        Return the value for the given key.
        Usage: mt=MonitoringTime()
               mt.get('ct_proxy')
        The method throws an AttributeError in case of no such key.

        :param key: name of key (string).
        :return: key value (int).
        """

        return getattr(self, key)
