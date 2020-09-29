#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

import os
import threading

from pilot.util.config import config
from pilot.common.pluginfactory import PluginFactory
from pilot.control.job import create_job
from pilot.eventservice.communicationmanager.communicationmanager import CommunicationManager
import logging
logger = logging.getLogger(__name__)

"""
Base Executor with one process to manage EventService
"""


class BaseExecutor(threading.Thread, PluginFactory):

    def __init__(self, **kwargs):
        super(BaseExecutor, self).__init__()
        self.setName("BaseExecutor")
        self.queue = None
        self.payload = None

        self.args = None
        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.__stop = threading.Event()

        self.__event_ranges = []
        self.__is_set_payload = False
        self.__is_retrieve_payload = False

        self.communication_manager = None

        self.proc = None

    def get_pid(self):
        return self.proc.pid if self.proc else None

    def __del__(self):
        self.stop()
        if self.communication_manager:
            self.communication_manager.stop()

    def is_payload_started(self):
        return False

    def start(self):
        super(BaseExecutor, self).start()
        self.communication_manager = CommunicationManager()
        self.communication_manager.start()

    def stop(self):
        if not self.is_stop():
            self.__stop.set()

    def is_stop(self):
        return self.__stop.is_set()

    def stop_communicator(self):
        logger.info("Stopping communication manager")
        if self.communication_manager:
            while self.communication_manager.is_alive():
                if not self.communication_manager.is_stop():
                    self.communication_manager.stop()
        logger.info("Communication manager stopped")

    def set_payload(self, payload):
        self.payload = payload
        self.__is_set_payload = True
        job = self.get_job()
        if job and job.workdir:
            os.chdir(job.workdir)

    def is_set_payload(self):
        return self.__is_set_payload

    def set_retrieve_payload(self):
        self.__is_retrieve_payload = True

    def is_retrieve_payload(self):
        return self.__is_retrieve_payload

    def retrieve_payload(self):
        logger.info("Retrieving payload: %s" % self.args)
        jobs = self.communication_manager.get_jobs(njobs=1, args=self.args)
        logger.info("Received jobs: %s" % jobs)
        if jobs:
            job = create_job(jobs[0], queue=self.queue)

            # get the payload command from the user specific code
            pilot_user = os.environ.get('PILOT_USER', 'atlas').lower()
            user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
            cmd = user.get_payload_command(job)
            logger.info("payload execution command: %s" % cmd)

            payload = {'executable': cmd,
                       'workdir': job.workdir,
                       'job': job}
            logger.info("Retrieved payload: %s" % payload)
            return payload
        return None

    def get_payload(self):
        if self.__is_set_payload:
            return self.payload

    def get_job(self):
        return self.payload['job'] if self.payload and 'job' in list(self.payload.keys()) else None  # Python 2/3

    def get_event_ranges(self, num_event_ranges=1, queue_factor=2):
        if config.Payload.executor_type.lower() == 'raythena':
            old_queue_factor = queue_factor
            queue_factor = 1
            logger.info("raythena - Changing queue_factor from %s to %s" % (old_queue_factor, queue_factor))
        logger.info("Getting event ranges: (num_ranges: %s) (queue_factor: %s)" % (num_event_ranges, queue_factor))
        if len(self.__event_ranges) < num_event_ranges:
            ret = self.communication_manager.get_event_ranges(num_event_ranges=num_event_ranges * queue_factor, job=self.get_job())
            for event_range in ret:
                self.__event_ranges.append(event_range)

        ret = []
        for _ in range(num_event_ranges):
            if len(self.__event_ranges) > 0:
                event_range = self.__event_ranges.pop(0)
                ret.append(event_range)
        logger.info("Received event ranges(num:%s): %s" % (len(ret), ret))
        return ret

    def update_events(self, messages):
        logger.info("Updating event ranges: %s" % messages)
        ret = self.communication_manager.update_events(messages)
        logger.info("Updated event ranges status: %s" % ret)
        return ret

    def update_jobs(self, jobs):
        logger.info("Updating jobs: %s" % jobs)
        ret = self.communication_manager.update_jobs(jobs)
        logger.info("Updated jobs status: %s" % ret)
        return ret

    def run(self):
        """
        Main run process
        """
        raise NotImplemented()
