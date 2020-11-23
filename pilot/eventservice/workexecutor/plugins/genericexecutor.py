#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2019
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

import json
import os
import time
import traceback

from pilot.api.es_data import StageOutESClient
from pilot.common.exception import PilotException, StageOutFailure

from pilot.common.errorcodes import ErrorCodes
from pilot.eventservice.esprocess.esprocess import ESProcess
from pilot.info.filespec import FileSpec
from pilot.info import infosys
from pilot.util.container import execute

from .baseexecutor import BaseExecutor

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()

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

    def is_payload_started(self):
        return self.proc.is_payload_started() if self.proc else False

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

    def tarzip_output_es(self):
        """
        Tar/zip eventservice outputs.

        :return: out_messages, output_file
        """

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
                    logger.error("Failed to add event output to tar/zip file: out_message: "
                                 "%s, exit_code: %s, stdout: %s, stderr: %s" % (out_msg, exit_code, stdout, stderr))
                    if 'retries' in out_msg and out_msg['retries'] >= 3:
                        logger.error("Discard out messages because it has been retried more than 3 times: %s" % out_msg)
                    else:
                        if 'retries' in out_msg:
                            out_msg['retries'] += 1
                        else:
                            out_msg['retries'] = 1
                        self.__queued_out_messages.append(out_msg)
        except Exception as e:
            logger.error("Failed to tar/zip event ranges: %s" % str(e))
            self.__queued_out_messages += out_messages
            return None, None

        return ret_messages, output_file

    def stageout_es_real(self, output_file):
        """
        Stage out event service output file.

        :param output_file: output file name.
        """

        job = self.get_job()
        logger.info('prepare to stage-out eventservice files')

        error = None
        file_data = {'scope': 'transient',
                     'lfn': os.path.basename(output_file),
                     }
        file_spec = FileSpec(filetype='output', **file_data)
        xdata = [file_spec]
        kwargs = dict(workdir=job.workdir, cwd=job.workdir, usecontainer=False, job=job)

        try_failover = False
        activity = ['es_events', 'pw']  ## FIX ME LATER: replace `pw` with `write_lan` once AGIS is updated (acopytools)

        try:
            client = StageOutESClient(job.infosys, logger=logger)
            try_failover = True

            client.prepare_destinations(xdata, activity)  ## IF ES job should be allowed to write only at `es_events` astorages, then fix activity names here
            client.transfer(xdata, activity=activity, **kwargs)
        except PilotException as error:
            logger.error(error.get_detail())
        except Exception as e:
            logger.error(traceback.format_exc())
            error = StageOutFailure("stageOut failed with error=%s" % e)

        logger.info('Summary of transferred files:')
        logger.info(" -- lfn=%s, status_code=%s, status=%s" % (file_spec.lfn, file_spec.status_code, file_spec.status))

        if error:
            logger.error('Failed to stage-out eventservice file(%s): error=%s' % (output_file, error.get_detail()))
        elif file_spec.status != 'transferred':
            msg = 'Failed to stage-out ES file(%s): logic corrupted: unknown internal error, fspec=%s' % (output_file, file_spec)
            logger.error(msg)
            raise StageOutFailure(msg)

        failover_storage_activity = ['es_failover', 'pw']

        if try_failover and error and error.get_error_code() not in [ErrorCodes.MISSINGOUTPUTFILE]:  ## try to failover to other storage

            xdata2 = [FileSpec(filetype='output', **file_data)]

            try:
                client.prepare_destinations(xdata2, failover_storage_activity)
                if xdata2[0].ddmendpoint != xdata[0].ddmendpoint:  ## skip transfer to same output storage
                    msg = 'Will try to failover ES transfer to astorage with activity=%s, rse=%s' % (failover_storage_activity, xdata2[0].ddmendpoint)
                    logger.info(msg)
                    client.transfer(xdata2, activity=activity, **kwargs)

                    logger.info('Summary of transferred files (failover transfer):')
                    logger.info(" -- lfn=%s, status_code=%s, status=%s" % (xdata2[0].lfn, xdata2[0].status_code, xdata2[0].status))

            except PilotException as e:
                if e.get_error_code() == ErrorCodes.NOSTORAGE:
                    logger.info('Failover ES storage is not defined for activity=%s .. skipped' % failover_storage_activity)
                else:
                    logger.error('Transfer to failover storage=%s failed .. skipped, error=%s' % (xdata2[0].ddmendpoint, e.get_detail()))
            except Exception as e:
                logger.error('Failover ES stageout failed .. skipped')
                logger.error(traceback.format_exc())

            if xdata2[0].status == 'transferred':
                error = None
                file_spec = xdata2[0]

        if error:
            raise error

        storage_id = infosys.get_storage_id(file_spec.ddmendpoint)

        return file_spec.ddmendpoint, storage_id, file_spec.filesize, file_spec.checksum

    def stageout_es(self, force=False):
        """
        Stage out event service outputs.

        """

        job = self.get_job()
        if len(self.__queued_out_messages):
            if force or self.__last_stageout_time is None or (time.time() > self.__last_stageout_time + job.infosys.queuedata.es_stageout_gap):

                out_messagess, output_file = self.tarzip_output_es()
                logger.info("tar/zip event ranges: %s, output_file: %s" % (out_messagess, output_file))

                if out_messagess:
                    self.__last_stageout_time = time.time()
                    try:
                        logger.info("Staging output file: %s" % output_file)
                        storage, storage_id, fsize, checksum = self.stageout_es_real(output_file)
                        logger.info("Staged output file (%s) to storage: %s storage_id: %s" % (output_file, storage, storage_id))

                        self.update_finished_event_ranges(out_messagess, output_file, fsize, checksum, storage_id)
                    except Exception as e:
                        logger.error("Failed to stage out file(%s): %s, %s" % (output_file, str(e), traceback.format_exc()))

                        if force:
                            self.update_failed_event_ranges(out_messagess)
                        else:
                            logger.info("Failed to stageout, adding messages back to the queued messages")
                            self.__queued_out_messages += out_messagess

    def clean(self):
        """
        Clean temp produced files
        """

        for msg in self.__all_out_messages:
            if msg['status'] in ['failed', 'fatal']:
                pass
            elif 'output' in msg:
                try:
                    logger.info("Removing es premerge file: %s" % msg['output'])
                    os.remove(msg['output'])
                except Exception as e:
                    logger.error("Failed to remove file(%s): %s" % (msg['output'], str(e)))
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
            logger.info("starting ES GenericExecutor with thread ident: %s" % (self.ident))
            if self.is_set_payload():
                payload = self.get_payload()
            elif self.is_retrieve_payload():
                payload = self.retrieve_payload()
            else:
                logger.error("Payload is not set but is_retrieve_payload is also not set. No payloads.")

            logger.info("payload: %s" % payload)

            logger.info("Starting ESProcess")
            proc = ESProcess(payload)
            self.proc = proc
            logger.info("ESProcess initialized")

            proc.set_get_event_ranges_hook(self.get_event_ranges)
            proc.set_handle_out_message_hook(self.handle_out_message)

            logger.info('ESProcess starts to run')
            proc.start()
            logger.info('ESProcess started to run')

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
            logger.error('Execute payload failed: %s, %s' % (str(e), traceback.format_exc()))
            self.clean()
            self.exit_code = -1
        logger.info('ES generic executor finished')
