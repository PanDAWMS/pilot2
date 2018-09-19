#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018


import ConfigParser
import datetime
import json
import os
import threading
import traceback

from pilot.common import exception
from ..communicationmanager import CommunicationResponse
from .basecommunicator import BaseCommunicator

import logging
logger = logging.getLogger(__name__)

"""
Harvester Share File Communicator
"""


class HarvesterShareFileCommunicator(BaseCommunicator):
    def __init__(self, *args, **kwargs):
        super(HarvesterShareFileCommunicator, self).__init__(args, kwargs)
        self.get_jobs_lock = threading.Lock()
        self.get_events_lock = threading.Lock()
        self.update_events_lock = threading.Lock()
        self.update_jobs_lock = threading.Lock()

        self.conf_file = None
        self.read_conf()

    def read_conf(self):
        self.conf_section = 'HarvesterShareFile'
        self.conf_map = {'jobRequestFile': 'worker_requestjob.json',
                         'jobSpecFile': 'HPCJobs.json',
                         'jobReportFile': 'jobReport.json',
                         'eventRequestFile': 'worker_requestevents.json',
                         'eventRangesFile': 'JobsEventRanges.json',
                         'updateEventsFile': 'worker_updateevents.json',
                         'eventStatusDumpJsonFile': 'event_status.dump.json'}

        if self.conf_file:
            config = ConfigParser.ConfigParser()
            config.read(self.conf_file)
            if config.has_section(self.conf_section):
                for key in self.conf_map:
                    if config.has_option(self.conf_section, key):
                        self.conf_map[key] = config.get(self.conf_section, key)

    def pre_check_get_jobs(self, req=None):
        """
        Precheck whether it's ok to send a requst to get jobs.
        """
        if os.path.exists(self.conf_map['jobRequestFile']):
            return CommunicationResponse({'status': -1})
        else:
            return CommunicationResponse({'status': 0})

    def request_get_jobs(self, req):
        """
        Send a requst to get jobs.
        """
        data = {'num_jobs': req.num_jobs}
        with open(self.conf_map['jobRequestFile'], 'w') as f:
            f.write(json.dumps(data))
        return CommunicationResponse({'status': 0})

    def check_get_jobs_status(self, req=None):
        """
        Check whether jobs are prepared
        """
        if os.path.exists(self.conf_map['jobSpecFile']):
            return CommunicationResponse({'status': 0})
        else:
            return CommunicationResponse({'status': -1})

    def get_jobs(self, req):
        """
        Get the job definition from panda server.

        :return: job definiton dictionary.
        """
        self.get_jobs_lock.acquire()

        resp = None
        try:
            logger.info("Loading job spec fil: %s" % self.conf_map['jobSpecFile'])
            with open(self.conf_map['jobSpecFile']) as f:
                jobs = json.load(f)
                logger.info("Loaded jobs(num: %s): %s" % (len(jobs), jobs))
                resp_attrs = {'status': 0, 'content': jobs, 'exception': None}
                resp = CommunicationResponse(resp_attrs)
            new_file = self.conf_map['jobSpecFile'] + '.' + datetime.utcnow().strftime("%Y%m%d_%H_%M%_%s")
            os.rename(self.conf_map['jobSpecFile'], new_file)
            logger.info("Rename job spec file %s to file %s" % (self.conf_map['jobSpecFile'], new_file))
        except Exception, e:
            logger.error("Failed to get jobs: %s, %s" % (e, traceback.format_exc()))
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to get jobs: %s" % (traceback.format_exc()))}
            resp = CommunicationResponse(resp_attrs)

        self.get_jobs_lock.release()

        return resp

    def pre_check_get_events(self, req=None):
        """
        Precheck whether it's ok to send a request to get events.
        """
        if os.path.exists(self.conf_map['eventRequestFile']):
            return CommunicationResponse({'status': -1})
        else:
            return CommunicationResponse({'status': 0})

    def request_get_events(self, req):
        """
        Send a requst to get events.
        """
        data = {'num_ranges': req.num_event_ranges}
        with open(self.conf_map['eventRequestFile'], 'w') as f:
            f.write(json.dumps(data))
        return CommunicationResponse({'status': 0})

    def check_get_events_status(self, req=None):
        """
        Check whether events prepared
        """
        if os.path.exists(self.conf_map['eventRangesFile']):
            return CommunicationResponse({'status': 0})
        else:
            return CommunicationResponse({'status': -1})

    def get_events(self, req):
        """
        Get events
        """
        self.get_events_lock.acquire()

        resp = None
        try:
            logger.info("Loading event ranges fil: %s" % self.conf_map['eventRangesFile'])
            with open(self.conf_map['eventRangesFile']) as f:
                events = json.load(f)
                logger.info("Loaded events(num:%s): %s" % (len(events), events))
                resp_attrs = {'status': 0, 'content': events, 'exception': None}
                resp = CommunicationResponse(resp_attrs)
            new_file = self.conf_map['eventRangesFile'] + '.' + datetime.utcnow().strftime("%Y%m%d_%H_%M%_%s")
            os.rename(self.conf_map['eventRangesFile'], new_file)
            logger.info("Rename event ranges file %s to file %s" % (self.conf_map['eventRangesFile'], new_file))
        except Exception, e:
            logger.error("Failed to download event ranges: %s, %s" % (e, traceback.format_exc()))
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to get events: %s" % (traceback.format_exc()))}
            resp = CommunicationResponse(resp_attrs)

        self.get_events_lock.release()

        return resp

    def pre_check_update_events(self, req=None):
        """
        Precheck whether it's ok to update events.
        """
        self.update_events_lock.acquire()
        status = -1
        try:
            if not os.path.exists(self.conf_map['eventStatusDumpJsonFile']):
                status = 0
        except Exception, e:
            logger.error("Failed to pre_check_update_events: %s, %s" % (e, traceback.format_exc()))
        self.update_events_lock.release()
        return CommunicationResponse({'status': status})

    def update_events(self, req):
        """
        Update events.
        """
        self.update_events_lock.acquire()

        resp = None
        try:
            logger.info("Updating events: %s" % req)
            data = req.update_events
            with open(self.conf_map['eventStatusDumpJsonFile'], 'w') as f:
                f.write(json.dumps(data))

            resp_attrs = {'status': 0, 'content': None, 'exception': None}
            resp = CommunicationResponse(resp_attrs)
        except Exception, e:
            logger.error("Failed to update event ranges: %s, %s" % (e, traceback.format_exc()))
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to update events: %s" % (traceback.format_exc()))}
            resp = CommunicationResponse(resp_attrs)

        self.update_events_lock.release()
        return resp

    def pre_check_update_jobs(self, req=None):
        """
        Precheck whether it's ok to update jobs.
        """
        self.update_jobs_lock.acquire()
        status = -1
        try:
            if not os.path.exists(self.conf_map['jobReportFile']):
                status = 0
        except Exception, e:
            logger.error("Failed to pre_check_update_jobs: %s, %s" % (e, traceback.format_exc()))
        self.update_jobs_lock.release()
        return CommunicationResponse({'status': status})

    def update_jobs(self, req):
        """
        Update jobs.
        """
        self.update_jobs_lock.acquire()

        resp = None
        try:
            logger.info("Updating jobs: %s" % req)
            data = req.jobs
            with open(self.conf_map['jobReportFile'], 'w') as f:
                f.write(json.dumps(data))

            resp_attrs = {'status': 0, 'content': None, 'exception': None}
            resp = CommunicationResponse(resp_attrs)
        except Exception, e:
            logger.error("Failed to update jobs: %s, %s" % (e, traceback.format_exc()))
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to update jobs: %s" % (traceback.format_exc()))}
            resp = CommunicationResponse(resp_attrs)

        self.update_jobs_lock.release()
        return resp
