#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

from pilot.common import exception

import logging
logger = logging.getLogger(__name__)

"""
Base communicator
"""


class BaseCommunicator(object):
    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance

    def __init__(self, *args, **kwargs):
        super(BaseCommunicator, self).__init__()
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def pre_check_get_jobs(self, req):
        """
        Precheck whether it's ok to send a requst to get jobs.
        """
        raise exception.NotImplemented()

    def request_get_jobs(self, req):
        """
        Send a requst to get jobs.
        """
        raise exception.NotImplemented()

    def check_get_jobs_status(self, req):
        """
        Check whether jobs are prepared
        """
        raise exception.NotImplemented()

    def get_jobs(self, req):
        """
        Get the job
        """
        raise exception.NotImplemented()

    def update_jobs(self, req):
        """
        Update jobs status.
        """
        raise exception.NotImplemented()

    def pre_check_get_events(self, req):
        """
        Precheck whether it's ok to send a request to get events.
        """
        raise exception.NotImplemented()

    def request_get_events(self, req):
        """
        Send a requst to get events.
        """
        raise exception.NotImplemented()

    def check_get_events_status(self, req):
        """
        Check whether events prepared
        """
        raise exception.NotImplemented()

    def get_events(self, req):
        """
        Get events
        """
        raise exception.NotImplemented()

    def pre_check_update_events(self, req):
        """
        Precheck whether it's ok to update events.
        """
        raise exception.NotImplemented()

    def update_events(self, req):
        """
        Update events.
        """
        raise exception.NotImplemented()

    def pre_check_update_jobs(self, req):
        """
        Precheck whether it's ok to update event ranges.
        """
        raise exception.NotImplemented()
