#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018


import commands
import os
import time
import signal

from pilot.common import exception
from pilot.control.payloads import generic
from pilot.eventservice.esprocess.eshook import ESHook
from pilot.eventservice.esprocess.esprocess import ESProcess
from pilot.eventservice.eventrange import download_event_ranges, update_event_ranges
from pilot.info import infosys
from pilot.util.auxiliary import get_logger

import logging
logger = logging.getLogger(__name__)


class Executor(generic.Executor, ESHook):
    def __init__(self, args, job, out, err):
        super(Executor, self).__init__(args, job, out, err)

        self.__event_ranges = []
        self.__queued_out_messages = []
        self.__last_stageout_time = None
        self.__all_out_messages = []

    def get_event_ranges(self, num_ranges=1):
        """
        Get event ranges hook function for tests.

        :returns: dict of event ranges.
                  None if no available events.
        """
        job = self.get_job()
        log = get_logger(job.jobid)
        log.info("Getting event ranges: (num_ranges: %s)" % num_ranges)
        if len(self.__event_ranges) < num_ranges:
            ret = download_event_ranges(job, num_ranges=infosys.queuedata.corecount)
            for event_range in ret:
                self.__event_ranges.append(event_range)

        ret = []
        for _ in range(num_ranges):
            if len(self.__event_ranges) > 0:
                event_range = self.__event_ranges.pop(0)
                ret.append(event_range)
        return ret

    def update_finished_event_ranges(self, out_messagess, output_file, fsize, adler32, storage_id):
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

        job = self.get_job()
        event_ranges = []
        for out_msg in out_messagess:
            event_ranges.append({"eventRangeID": out_msg['id'], "eventStatus": 'finished'})
        event_range_status = [{"zipFile": {"numEvents": len(event_ranges),
                                           "objstoreID": storage_id,
                                           "adler32": adler32,
                                           "lfn": os.path.basename(output_file),
                                           "fsize": fsize,
                                           "pathConvention": 1000},
                               "eventRanges": event_ranges}]
        update_event_ranges(job, event_range_status)

    def update_failed_event_ranges(self, out_messagess):
        """
        Update failed event ranges

        :param out_messages: messages from AthenaMP.
        """
        if len(out_messagess) == 0:
            return

        job = self.get_job()
        event_ranges = []
        for message in out_messagess:
            status = message['status'] if message['status'] in ['failed', 'fatal'] else 'failed'
            # ToBeFixed errorCode
            event_ranges.append({"errorCode": 1220, "eventRangeID": message['id'], "eventStatus": status})
        update_event_ranges(job, event_ranges, version=0)

    def handle_out_message(self, message):
        """
        Handle ES output or error messages hook function for tests.

        :param message: a dict of parsed message.
                        For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                        Fro 'failed' event ranges, it's {'id': <id>, 'status': 'failed', 'message': <full message>}.
        """
        job = self.get_job()
        log = get_logger(job.jobid)
        log.info("Handling out message: %s" % message)

        self.__all_out_messages.append(message)

        if message['status'] in ['failed', 'fatal']:
            self.update_failed_event_ranges([message])
        else:
            self.__queued_out_messages.append(message)

    def tarzip_output_es(self):
        """
        Tar/zip eventservice outputs.

        :return: out_messages, output_file, fsize, adler32
        """
        job = self.get_job()
        log = get_logger(job.jobid)

        out_messages = []
        while len(self.__queued_out_messages) > 0:
            out_messages.append(self.__queued_out_messages.pop())

        output_file = "EventService_premerge_%s.tar" % out_messages[0]['id']

        ret_messages = []
        try:
            for out_msg in out_messages:
                command = "tar -rf " + output_file + " --directory=%s %s" % (os.path.dirname(out_msg['output']), os.path.basename(out_msg['output']))
                status, output = commands.getstatusoutput(command)
                if status == 0:
                    ret_messages.append(out_msg)
                else:
                    log.error("Failed to add event output to tar/zip file: out_message: %s, error: %s" % (out_msg, output))
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
            return None, None, None, None

        # ToBeFixed
        fsize = 111
        adler32 = "12345678"

        return ret_messages, output_file, fsize, adler32

    def stageout_es_real(self, outupt_file):
        """
        Stage out event service output file.

        :param output_file: output file name.
        """
        raise exception.NotImplemented("stageout_es_real is not implemented")

    def stageout_es(self, force=False):
        """
        Stage out event service outputs.

        """

        if len(self.__queued_out_messages):
            if force or self.__last_stageout_time is None or (time.time() > self.__last_stageout_time + infosys.queuedata.es_stageout_gap):
                job = self.get_job()
                log = get_logger(job.jobid)

                out_messagess, output_file, fsize, adler32 = self.tarzip_output_es()
                log.info("tar/zip event ranges: %s, output_file: %s, fsize: %s, adler32: %s" % (out_messagess, output_file, fsize, adler32))

                if out_messagess:
                    self.__last_stageout_time = time.time()
                    try:
                        log.info("Staging output file: %s" % output_file)
                        storage, storage_id = self.stageout_es_real(output_file)
                        log.info("Staged output file (%s) to storage: %s storage_id: %s" % (storage, storage_id))

                        self.update_finished_event_ranges(out_messagess, output_file, fsize, adler32, storage_id)
                    except Exception as e:
                        log.error("Failed to stage out file(%s): %s" % (output_file, str(e)))

                        log.info("Failed to stageout, adding messages back to the queued messages")
                        self.__queued_out_messages += out_messagess
                        self.update_failed_event_ranges(out_messagess)

    def clean(self):
        """
        Clean temp produced files
        """
        job = self.get_job()
        log = get_logger(job.jobid)

        for msg in self.__all_out_messages:
            if msg['status'] in ['failed', 'fatal']:
                pass
            elif 'output' in msg:
                try:
                    log.info("Removing es premerge file: %s" % msg['output'])
                    os.remove(msg['output'])
                except Exception as e:
                    log.error("Failed to remove file(%s): %s" % (msg['output'], str(e)))
        self.__event_ranges = []
        self.__queued_out_messages = []
        self.__last_stageout_time = None
        self.__all_out_messages = []

    def run_payload(self, job, out, err):
        """
        (add description)

        :param job:
        :param out:
        :param err:
        :return:
        """
        log = get_logger(job.jobid)

        # get the payload command from the user specific code
        # cmd = get_payload_command(job, queuedata)
        athena_version = job.homepackage.split('/')[1]
        asetup = 'source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh --quiet; '\
                 'source $AtlasSetup/scripts/asetup.sh %s,here; ' % athena_version
        cmd = job.transformation + ' ' + job.jobparams

        executable = "export ATHENA_PROC_NUMBER=%s; " % infosys.queuedata.corecount
        executable = executable + asetup + cmd
        log.debug('executable=%s' % executable)

        try:
            payload = {'executable': executable, 'workdir': job.workdir, 'output_file': out, 'error_file': err}
            log.debug("payload: %s" % payload)

            log.info("Starting ESProcess")
            proc = ESProcess(payload)
            log.info("ESProcess started")

            proc.set_get_event_ranges_hook(self.get_event_ranges)
            proc.set_handle_out_message_hook(self.handle_out_message)

            proc.start()
        except Exception as e:
            log.error('could not execute: %s' % str(e))
            return None

        log.info('started -- pid=%s executable=%s' % (proc.pid, asetup + cmd))

        return proc

    def wait_graceful(self, args, proc, job):
        """
        (add description)

        :param args:
        :param proc:
        :param job:
        :return:
        """

        log = get_logger(job.jobid)

        breaker = False
        exit_code = None
        iteration = 0L
        while proc.isAlive():
            iteration += 1
            for i in xrange(100):
                if args.graceful_stop.is_set():
                    breaker = True
                    log.debug('breaking -- sending SIGTERM pid=%s' % proc.pid)
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    # proc.terminate()
                    break
                time.sleep(0.1)
            if breaker:
                log.debug('breaking -- sleep 3s before sending SIGKILL pid=%s' % proc.pid)
                time.sleep(3)
                proc.kill()
                break

            self.stageout_es()

            exit_code = proc.poll()
            if iteration % 10 == 0:
                log.info('running: iteration=%d pid=%s exit_code=%s' % (iteration, proc.pid, exit_code))

        self.stageout_es(force=True)
        self.clean()

        exit_code = proc.poll()
        return exit_code
