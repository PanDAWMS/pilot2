#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

import json
import threading
import traceback

from pilot.common import exception
from pilot.util import https
from pilot.util.config import config
from ..communicationmanager import CommunicationResponse
from .basecommunicator import BaseCommunicator

import logging
logger = logging.getLogger(__name__)

"""
Panda Communicator
"""


class PandaCommunicator(BaseCommunicator):
    def __init__(self, *args, **kwargs):
        super(PandaCommunicator, self).__init__(args, kwargs)
        self.get_jobs_lock = threading.Lock()
        self.get_events_lock = threading.Lock()
        self.update_events_lock = threading.Lock()
        self.update_jobs_lock = threading.Lock()

    def pre_check_get_jobs(self, req=None):
        """
        Precheck whether it's ok to send a requst to get jobs.
        """
        return CommunicationResponse({'status': 0})

    def request_get_jobs(self, req):
        """
        Send a requst to get jobs.
        """
        return CommunicationResponse({'status': 0})

    def check_get_jobs_status(self, req=None):
        """
        Check whether jobs are prepared
        """
        return CommunicationResponse({'status': 0})

    def get_jobs(self, req):
        """
        Get the job definition from panda server.

        :return: job definiton dictionary.
        """

        self.get_jobs_lock.acquire()

        try:
            jobs = []
            resp_attrs = None

            data = {'getProxyKey': 'False'}
            kmap = {'node': 'node', 'mem': 'mem', 'getProxyKey': 'getProxyKey', 'computingElement': 'queue', 'diskSpace': 'disk_space',
                    'siteName': 'site', 'prodSourceLabel': 'job_label', 'workingGroup': 'working_group', 'cpu': 'cpu'}
            for key, value in list(kmap.items()):  # Python 2/3
                if hasattr(req, value):
                    data[key] = getattr(req, value)

            for i in range(req.num_jobs):
                logger.info("Getting jobs: %s" % data)
                res = https.request('{pandaserver}/server/panda/getJob'.format(pandaserver=config.Pilot.pandaserver),
                                    data=data)
                logger.info("Got jobs returns: %s" % res)

                if res is None:
                    resp_attrs = {'status': None, 'content': None, 'exception': exception.CommunicationFailure("Get job failed to get response from Panda.")}
                    break
                elif res['StatusCode'] == 20 and 'no jobs in PanDA' in res['errorDialog']:
                    resp_attrs = {'status': res['StatusCode'],
                                  'content': None,
                                  'exception': exception.CommunicationFailure("No jobs in panda")}
                elif res['StatusCode'] != 0:
                    resp_attrs = {'status': res['StatusCode'],
                                  'content': None,
                                  'exception': exception.CommunicationFailure("Get job from Panda returns a non-zero value: %s" % res['StatusCode'])}
                    break
                else:
                    jobs.append(res)

            if jobs:
                resp_attrs = {'status': 0, 'content': jobs, 'exception': None}
            elif not resp_attrs:
                resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to get jobs")}

            resp = CommunicationResponse(resp_attrs)
        except Exception as e:  # Python 2/3
            logger.error("Failed to get jobs: %s, %s" % (e, traceback.format_exc()))
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to get jobs: %s" % (traceback.format_exc()))}
            resp = CommunicationResponse(resp_attrs)

        self.get_jobs_lock.release()

        return resp

    def pre_check_get_events(self, req=None):
        """
        Precheck whether it's ok to send a request to get events.
        """
        return CommunicationResponse({'status': 0})

    def request_get_events(self, req):
        """
        Send a requst to get events.
        """
        return CommunicationResponse({'status': 0})

    def check_get_events_status(self, req=None):
        """
        Check whether events prepared
        """
        return CommunicationResponse({'status': 0})

    def get_events(self, req):
        """
        Get events
        """
        self.get_events_lock.acquire()

        resp = None
        try:
            if not req.num_ranges:
                # ToBeFix num_ranges with corecount
                req.num_ranges = 1

            data = {'pandaID': req.jobid,
                    'jobsetID': req.jobsetid,
                    'taskID': req.taskid,
                    'nRanges': req.num_ranges}

            logger.info("Downloading new event ranges: %s" % data)
            res = https.request('{pandaserver}/server/panda/getEventRanges'.format(pandaserver=config.Pilot.pandaserver),
                                data=data)
            logger.info("Downloaded event ranges: %s" % res)

            if res is None:
                resp_attrs = {'status': -1,
                              'content': None,
                              'exception': exception.CommunicationFailure("Get events from panda returns None as return value")}
            elif res['StatusCode'] == 0 or str(res['StatusCode']) == '0':
                resp_attrs = {'status': 0, 'content': res['eventRanges'], 'exception': None}
            else:
                resp_attrs = {'status': res['StatusCode'],
                              'content': None,
                              'exception': exception.CommunicationFailure("Get events from panda returns non-zero value: %s" % res['StatusCode'])}

            resp = CommunicationResponse(resp_attrs)
        except Exception as e:  # Python 2/3
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
        try:
            pass
        except Exception as e:  # Python 2/3
            logger.error("Failed to pre_check_update_events: %s, %s" % (e, traceback.format_exc()))
        self.update_events_lock.release()
        return CommunicationResponse({'status': 0})

    def update_events(self, req):
        """
        Update events.
        """
        self.update_events_lock.acquire()

        resp = None
        try:
            logger.info("Updating events: %s" % req)

            res = https.request('{pandaserver}/server/panda/updateEventRanges'.format(pandaserver=config.Pilot.pandaserver),
                                data=req.update_events)

            logger.info("Updated event ranges status: %s" % res)
            resp_attrs = {'status': 0, 'content': res, 'exception': None}
            resp = CommunicationResponse(resp_attrs)
        except Exception as e:  # Python 2/3
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
        try:
            pass
        except Exception as e:  # Python 2/3
            logger.error("Failed to pre_check_update_jobs: %s, %s" % (e, traceback.format_exc()))
        self.update_jobs_lock.release()
        return CommunicationResponse({'status': 0})

    def update_job(self, job):
        """
        Update job.
        """

        try:
            logger.info("Updating job: %s" % job)
            res = https.request('{pandaserver}/server/panda/updateJob'.format(pandaserver=config.Pilot.pandaserver),
                                data=job)

            logger.info("Updated jobs status: %s" % res)
            return res
        except Exception as e:  # Python 2/3
            logger.error("Failed to update jobs: %s, %s" % (e, traceback.format_exc()))
            return -1

    def update_jobs(self, req):
        """
        Update jobs.
        """
        self.update_jobs_lock.acquire()

        resp = None
        try:
            logger.info("Updating jobs: %s" % req)
            res_list = []
            for job in req.jobs:
                res = self.update_job(job)
                res_list.append(res)
            resp_attrs = {'status': 0, 'content': res_list, 'exception': None}
            resp = CommunicationResponse(resp_attrs)
        except Exception as e:  # Python 2/3
            logger.error("Failed to update jobs: %s, %s" % (e, traceback.format_exc()))
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to update jobs: %s" % (traceback.format_exc()))}
            resp = CommunicationResponse(resp_attrs)

        self.update_jobs_lock.release()
        return resp

    def update_jobs_old(self, req):
        """
        Update jobs.
        """
        self.update_jobs_lock.acquire()

        resp = None
        try:
            logger.info("Updating jobs: %s" % req)
            data = {'jobList': json.dumps(req.jobs)}

            res = https.request('{pandaserver}/server/panda/updateJobsInBulk'.format(pandaserver=config.Pilot.pandaserver),
                                data=data)

            logger.info("Updated jobs status: %s" % res)
            resp_attrs = {'status': 0, 'content': res, 'exception': None}
            resp = CommunicationResponse(resp_attrs)
        except Exception as e:  # Python 2/3
            logger.error("Failed to update jobs: %s, %s" % (e, traceback.format_exc()))
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to update jobs: %s" % (traceback.format_exc()))}
            resp = CommunicationResponse(resp_attrs)

        self.update_jobs_lock.release()
        return resp
