#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018

import json
import os
import time
import traceback

from pilot.api.data import StageOutClient
from pilot.common import exception
from pilot.eventservice.esprocess.esprocess import ESProcess
from pilot.info.filespec import FileSpec
from pilot.util.auxiliary import get_logger
from pilot.util.container import execute
from .baseexecutor import BaseExecutor

import logging
logger = logging.getLogger(__name__)

"""
Generic Executor with one process to manage EventService
"""


class GenericExecutor(BaseExecutor):
    def __init__(self, **kwargs):
        super(GenericExecutor, self).__init__(**kwargs)
        self.setName("GenericExecutor")

        self.__queued_out_messages = []
        self.__last_stageout_time = None
        self.__all_out_messages = []

        self.proc = None
        self.exit_code = None

    def get_pid(self):
        return self.proc.pid if self.proc else None

    def get_exit_code(self):
        return self.exit_code

    def update_finished_event_ranges(self, out_messagess, output_file, fsize, checksum, storage_id):
        """
        Update finished event ranges

        :param out_messages: messages from AthenaMP.
        :param output_file: output file name.
        :param fsize: file size.
        :param adler32: checksum (adler32) of the file.
        :param storage_id: the id of the storage.
        """
        if len(out_messagess) == 0:
            return

        event_ranges = []
        for out_msg in out_messagess:
            event_ranges.append({"eventRangeID": out_msg['id'], "eventStatus": 'finished'})
        event_range_status = {"zipFile": {"numEvents": len(event_ranges),
                                          "objstoreID": storage_id,
                                          "lfn": os.path.basename(output_file),
                                          "fsize": fsize,
                                          "pathConvention": 1000},
                              "eventRanges": event_ranges}
        for checksum_key in checksum:
            event_range_status["zipFile"][checksum_key] = checksum[checksum_key]
        event_range_message = {'version': 1, 'eventRanges': json.dumps([event_range_status])}
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
            event_ranges.append({"errorCode": 1220, "eventRangeID": message['id'], "eventStatus": status})
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
        job = self.get_job()
        log = get_logger(job.jobid, logger)
        log.info("Handling out message: %s" % message)

        self.__all_out_messages.append(message)

        if message['status'] in ['failed', 'fatal']:
            self.update_failed_event_ranges([message])
        else:
            self.__queued_out_messages.append(message)

    def tarzip_output_es(self):
        """
        Tar/zip eventservice outputs.

        :return: out_messages, output_file
        """
        job = self.get_job()
        log = get_logger(job.jobid, logger)

        out_messages = []
        while len(self.__queued_out_messages) > 0:
            out_messages.append(self.__queued_out_messages.pop())

        output_file = "EventService_premerge_%s.tar" % out_messages[0]['id']

        ret_messages = []
        try:
            for out_msg in out_messages:
                command = "tar -rf " + output_file + " --directory=%s %s" % (os.path.dirname(out_msg['output']), os.path.basename(out_msg['output']))
                exit_code, stdout, stderr = execute(command)
                if exit_code == 0:
                    ret_messages.append(out_msg)
                else:
                    log.error("Failed to add event output to tar/zip file: out_message: %s, exit_code: %s, stdout: %s, stderr: %s" % (out_msg,
                                                                                                                                      exit_code,
                                                                                                                                      stdout,
                                                                                                                                      stderr))
                    if 'retries' in out_msg and out_msg['retries'] >= 3:
                        log.error("Discard out messages because it has been retried more than 3 times: %s" % out_msg)
                    else:
                        if 'retries' in out_msg:
                            out_msg['retries'] += 1
                        else:
                            out_msg['retries'] = 1
                        self.__queued_out_messages.append(out_msg)
        except Exception as e:
            log.error("Failed to tar/zip event ranges: %s" % str(e))
            self.__queued_out_messages += out_messages
            return None, None

        return ret_messages, output_file

    def stageout_es_real(self, output_file):
        """
        Stage out event service output file.

        :param output_file: output file name.
        """
        job = self.get_job()
        log = get_logger(job.jobid, logger)
        log.info('prepare to stage-out eventservice files')

        error = None
        try:
            file_data = {'scope': 'transient',
                         'lfn': os.path.basename(output_file),
                         }
            file_spec = FileSpec(type='output', **file_data)
            xdata = [file_spec]
            client = StageOutClient(job.infosys, logger=log)
            kwargs = dict(workdir=job.workdir, cwd=job.workdir, usecontainer=False, job=job)
            client.transfer(xdata, activity=['es_events', 'pw', 'es_failover'], **kwargs)
        except exception.PilotException, error:
            log.error(error.get_detail())
        except Exception, e:
            import traceback
            log.error(traceback.format_exc())
            error = exception.StageOutFailure("stageOut failed with error=%s" % e)

        log.info('Summary of transferred files:')
        log.info(" -- lfn=%s, status_code=%s, status=%s" % (file_spec.lfn, file_spec.status_code, file_spec.status))

        if error or file_spec.status != 'transferred':
            log.error('Failed to stage-out eventservice file(%s): error=%s' % (output_file, error.get_detail()))
            raise error

        return file_spec.ddmendpoint, file_spec.storage_id, file_spec.filesize, file_spec.checksum

    def stageout_es(self, force=False):
        """
        Stage out event service outputs.

        """
        job = self.get_job()
        log = get_logger(job.jobid, logger)
        if len(self.__queued_out_messages):
            if force or self.__last_stageout_time is None or (time.time() > self.__last_stageout_time + job.infosys.queuedata.es_stageout_gap):

                out_messagess, output_file = self.tarzip_output_es()
                log.info("tar/zip event ranges: %s, output_file: %s" % (out_messagess, output_file))

                if out_messagess:
                    self.__last_stageout_time = time.time()
                    try:
                        log.info("Staging output file: %s" % output_file)
                        storage, storage_id, fsize, checksum = self.stageout_es_real(output_file)
                        log.info("Staged output file (%s) to storage: %s storage_id: %s" % (output_file, storage, storage_id))

                        self.update_finished_event_ranges(out_messagess, output_file, fsize, checksum, storage_id)
                    except Exception as e:
                        log.error("Failed to stage out file(%s): %s, %s" % (output_file, str(e), traceback.format_exc()))

                        if force:
                            self.update_failed_event_ranges(out_messagess)
                        else:
                            log.info("Failed to stageout, adding messages back to the queued messages")
                            self.__queued_out_messages += out_messagess

    def clean(self):
        """
        Clean temp produced files
        """
        job = self.get_job()
        log = get_logger(job.jobid, logger)

        for msg in self.__all_out_messages:
            if msg['status'] in ['failed', 'fatal']:
                pass
            elif 'output' in msg:
                try:
                    log.info("Removing es premerge file: %s" % msg['output'])
                    os.remove(msg['output'])
                except Exception as e:
                    log.error("Failed to remove file(%s): %s" % (msg['output'], str(e)))
        self.__queued_out_messages = []
        self.__last_stageout_time = None
        self.__all_out_messages = []

        if self.proc:
            self.proc.stop(20)
            while self.proc.is_alive():
                time.sleep(0.1)

        self.stop_communicator()

    def run(self):
        """
        Initialize and run ESProcess.
        """
        try:
            if self.is_set_payload():
                payload = self.get_payload()
            elif self.is_retrieve_payload():
                payload = self.retrieve_payload()
            else:
                logger.error("Payload is not set but is_retrieve_payload is also not set. No payloads.")

            job = self.get_job()
            log = get_logger(job.jobid, logger)
            log.info("payload: %s" % payload)

            log.info("Starting ESProcess")
            proc = ESProcess(payload)
            self.proc = proc
            log.info("ESProcess initialized")

            proc.set_get_event_ranges_hook(self.get_event_ranges)
            proc.set_handle_out_message_hook(self.handle_out_message)

            log.info('ESProcess starts to run')
            proc.start()
            log.info('ESProcess started to run')

            exit_code = None
            iteration = 0L
            while proc.is_alive():
                iteration += 1
                if self.is_stop():
                    log.info('Stop is set. breaking -- stop process pid=%s' % proc.pid)
                    proc.stop(60)
                    break
                self.stageout_es()

                exit_code = proc.poll()
                if iteration % 300 == 0:
                    log.info('running: iteration=%d pid=%s exit_code=%s' % (iteration, proc.pid, exit_code))
                time.sleep(1)

            while proc.is_alive():
                time.sleep(0.1)
            log.info("ESProcess finished")

            self.stageout_es(force=True)
            self.clean()

            self.exit_code = proc.poll()

        except Exception as e:
            logger.error('Execute payload failed: %s, %s' % (str(e), traceback.format_exc()))
            self.clean()
            self.exit_code = -1
        logger.info('ES generic executor finished')
