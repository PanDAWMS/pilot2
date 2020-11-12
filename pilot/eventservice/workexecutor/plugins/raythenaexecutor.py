#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Miha Muskinja, miha.muskinja@cern.ch, 2020
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

import json
import os
import time
import traceback

from pilot.common.errorcodes import ErrorCodes
from pilot.eventservice.esprocess.esprocess import ESProcess
from pilot.info.filespec import FileSpec
from pilot.util.filehandling import calculate_checksum

from .baseexecutor import BaseExecutor

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()

"""
Raythena Executor with one process to manage EventService
"""


class RaythenaExecutor(BaseExecutor):
    def __init__(self, **kwargs):
        super(RaythenaExecutor, self).__init__(**kwargs)
        self.setName("RaythenaExecutor")

        self.__queued_out_messages = []
        self.__last_stageout_time = None
        self.__all_out_messages = []

        self.proc = None
        self.exit_code = None

    def is_payload_started(self):
        return self.proc.is_payload_started() if self.proc else False

    def get_pid(self):
        return self.proc.pid if self.proc else None

    def get_exit_code(self):
        return self.exit_code

    def create_file_spec(self, pfn):
        checksum = calculate_checksum(pfn)
        filesize = os.path.getsize(pfn)
        file_data = {'scope': 'transient',
                     'lfn': os.path.basename(pfn),
                     'checksum': checksum,
                     'filesize': filesize,
                     }
        file_spec = FileSpec(filetype='output', **file_data)
        return file_spec

    def update_finished_event_ranges(self, out_messagess):
        """
        Update finished event ranges

        :param out_messages: messages from AthenaMP.
        """

        logger.info("update_finished_event_ranges:")

        if len(out_messagess) == 0:
            return

        event_ranges = []
        for out_msg in out_messagess:
            fspec = self.create_file_spec(out_msg['output'])
            event_range_status = {"eventRangeID": out_msg['id'], "eventStatus": 'finished', "pfn": out_msg['output'], "fsize": fspec.filesize}
            for checksum_key in fspec.checksum:
                event_range_status[checksum_key] = fspec.checksum[checksum_key]
            event_ranges.append(event_range_status)
        event_ranges_status = {"esOutput": {"numEvents": len(event_ranges)}, "eventRanges": event_ranges}
        event_range_message = {'version': 1, 'eventRanges': json.dumps([event_ranges_status])}
        self.update_events(event_range_message)

        job = self.get_job()
        job.nevents += len(event_ranges)

    def update_failed_event_ranges(self, out_messagess):
        """
        Update failed event ranges

        :param out_messages: messages from AthenaMP.
        """
        if len(out_messagess) == 0:
            return

        event_ranges = []
        for message in out_messagess:
            status = message['status'] if message['status'] in ['failed', 'fatal'] else 'failed'
            # ToBeFixed errorCode
            event_ranges.append({"errorCode": errors.UNKNOWNPAYLOADFAILURE, "eventRangeID": message['id'], "eventStatus": status})
            event_range_message = {'version': 0, 'eventRanges': json.dumps(event_ranges)}
            self.update_events(event_range_message)

    def handle_out_message(self, message):
        """
        Handle ES output or error messages hook function for tests.

        :param message: a dict of parsed message.
                        For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                        Fro 'failed' event ranges, it's {'id': <id>, 'status': 'failed', 'message': <full message>}.
        """

        logger.info("Handling out message: %s" % message)

        self.__all_out_messages.append(message)

        if message['status'] in ['failed', 'fatal']:
            self.update_failed_event_ranges([message])
        else:
            self.__queued_out_messages.append(message)

    def stageout_es(self, force=False):
        """
        Stage out event service outputs.

        """

        job = self.get_job()
        logger.info("job.infosys.queuedata.es_stageout_gap: %s" % job.infosys.queuedata.es_stageout_gap)
        if len(self.__queued_out_messages):
            if force or self.__last_stageout_time is None or (time.time() > self.__last_stageout_time + job.infosys.queuedata.es_stageout_gap):
                out_messages = []
                while len(self.__queued_out_messages) > 0:
                    out_messages.append(self.__queued_out_messages.pop())
                self.update_finished_event_ranges(out_messages)

    def clean(self):
        """
        Clean temp produced files
        """

        logger.info("shutting down...")

        self.__queued_out_messages = []
        self.__last_stageout_time = None
        self.__all_out_messages = []

        if self.proc:
            self.proc.stop()
            while self.proc.is_alive():
                time.sleep(0.1)

        self.stop_communicator()

    def run(self):
        """
        Initialize and run ESProcess.
        """
        try:
            logger.info("starting ES RaythenaExecutor with thread ident: %s" % self.ident)
            if self.is_set_payload():
                payload = self.get_payload()
            elif self.is_retrieve_payload():
                payload = self.retrieve_payload()
            else:
                logger.error("Payload is not set but is_retrieve_payload is also not set. No payloads.")

            logger.info("payload: %s" % payload)

            logger.info("Starting ESProcess")
            proc = ESProcess(payload, waiting_time=999999)
            self.proc = proc
            logger.info("ESProcess initialized")

            proc.set_get_event_ranges_hook(self.get_event_ranges)
            proc.set_handle_out_message_hook(self.handle_out_message)

            logger.info('ESProcess starts to run')
            proc.start()
            logger.info('ESProcess started to run')

            exit_code = None
            try:
                iteration = long(0)  # Python 2
            except Exception:
                iteration = 0  # Python 3
            while proc.is_alive():
                iteration += 1
                if self.is_stop():
                    logger.info('Stop is set. breaking -- stop process pid=%s' % proc.pid)
                    proc.stop()
                    break
                self.stageout_es()

                exit_code = proc.poll()
                if iteration % 60 == 0:
                    logger.info('running: iteration=%d pid=%s exit_code=%s' % (iteration, proc.pid, exit_code))
                time.sleep(5)

            while proc.is_alive():
                time.sleep(1)
            logger.info("ESProcess finished")

            self.stageout_es(force=True)
            self.clean()

            self.exit_code = proc.poll()

        except Exception as e:
            logger.error('Execute payload failed: %s, %s' % (e, traceback.format_exc()))
            self.clean()
            self.exit_code = -1
        logger.info('ES raythena executor finished')
