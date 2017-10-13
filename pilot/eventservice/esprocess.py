# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017

import json
import logging
import os
import Queue
import re
import signal
import subprocess
import time

from pilot.common.exception import PilotException, MessageFailure, SetupFailure, RunPayloadFailure, UnKnownException
from pilot.eventservice.esmessage import MessageThread


logger = logging.getLogger(__name__)


class ESProcess():
    """
    Main EventService Process.
    """
    def __init__(self, payload):
        self.__message_queue = Queue.Queue()
        self.__payload = payload

        self.__message_thread = None
        self.__process = None

        self.get_event_ranges_hook = None
        self.handle_out_message_hook = None

        self.__monitor_log_time = None
        self.__no_more_event_time = None
        self.__waiting_time = 30 * 60

    def init_message_thread(self, socketname='EventService_EventRanges', context='local'):
        """
        init message thread.
        """

        logger.info("start to init message thread")
        try:
            self.__message_thread = MessageThread(self.__message_queue, socketname, context)
            self.__message_thread.start()
        except PilotException as e:
            logger.error("Failed to start message thread: %s" % e.get_detail())
        except Exception as e:
            logger.error("Failed to start message thread: %s" % str(e))
            raise MessageFailure(e)
        logger.info("finished to init message thread")

    def init_payload_process(self):
        """
        init payload process.
        """

        logger.info("start to init payload process")
        try:
            executable = self.__payload['executable']
            workdir = ''
            if 'workdir' in self.__payload:
                workdir = self.__payload['workdir']
                if not os.path.exists(workdir):
                    os.makedirs(workdir)
                elif not os.path.isdir(workdir):
                    raise SetupFailure('Workdir exists but it is not a directory.')
                executable = 'cd %s; %s' % (workdir, executable)
            output_file = self.__payload['output_file'] if 'output_file' in self.__payload else os.path.join(workdir, "ES_payload_output.txt")
            error_file = self.__payload['error_file'] if 'error_file' in self.__payload else os.path.join(workdir, "ES_payload_error.txt")
            output_file_fd = open(output_file, 'w')
            error_file_fd = open(error_file, 'w')
            self.__process = subprocess.Popen(executable, stdout=output_file_fd, stderr=error_file_fd, shell=True)
            logger.debug("Started new processs(executable: %s, stdout: %s, stderr: %s, pid: %s)" % (executable, output_file, error_file, self.__process.pid))
        except PilotException as e:
            logger.error("Failed to start payload process: %s" % e.get_detail())
        except Exception as e:
            logger.error("Failed to start payload process: %s" % str(e))
            raise SetupFailure(e)
        logger.info("finished to init payload process")

    def set_get_event_ranges_hook(self, hook):
        """
        set get_event_ranges hook.
        """

        self.get_event_ranges_hook = hook

    def get_get_event_ranges_hook(self):
        """
        get get_event_ranges hook.
        """

        return self.get_event_ranges_hook

    def set_handle_out_message_hook(self, hook):
        """
        set handle_out_message hook.
        """

        self.handle_out_message_hook = hook

    def get_handle_out_message_hook(self):
        """
        get handle_out_message hook.
        """

        return self.handle_out_message_hook

    def init(self):
        """
        initialization.
        """

        try:
            self.init_message_thread()
            self.init_payload_process()
        except Exception as e:
            # TODO: raise exceptions
            raise e

    def monitor(self):
        """
        Monitor whether a process is dead.

        raises: # TODO define different exceptions.
        """

        if self.__no_more_event_time and time.time() - self.__no_more_event_time > self.__waiting_time:
            raise Exception('Too long time(%s seconds) since "No more events" is injected' %
                            (time.time() - self.__no_more_event_time))

        if self.__monitor_log_time is None or self.__monitor_log_time < time.time() - 10 * 60:
            self.__monitor_log_time = time.time()
            logger.info('monitor is checking dead process.')

        if self.__message_thread is None:
            raise MessageFailure("Message thread has not start.")
        if not self.__message_thread.is_alive():
            raise MessageFailure("Message thread is not alive.")

        if self.__process is None:
            raise RunPayloadFailure("Payload Process has not start.")
        if self.__process.poll() is not None:
            raise RunPayloadFailure("Payload process is not alive: %s" % self.__process.poll())

    def get_event_ranges(self, num_ranges=1):
        """
        Get event ranges: get_event_ranges hook is called.
        """

        logger.debug('getting event ranges(num_ranges=%s)' % num_ranges)
        if not self.get_event_ranges_hook:
            raise SetupFailure("get_event_ranges_hook is not set")

        try:
            logger.debug('calling get_event_ranges hook(%s) to get event ranges.' % self.get_event_ranges_hook)
            event_ranges = self.get_event_ranges_hook(num_ranges)
            logger.debug('got event ranges: %s' % event_ranges)
            return event_ranges
        except:
            raise MessageFailure("Failed to get event ranges.")

    def send_event_ranges_to_payload(self, event_ranges):
        """
        Send event ranges to payload through message thread.
        """

        msg = None
        if "No more events" in event_ranges:
            msg = event_ranges
            self.__no_more_event_time = time.time()
        else:
            if type(event_ranges) is not list:
                event_ranges = [event_ranges]
            msg = json.dumps(event_ranges)
        logger.debug('send event ranges to payload: %s' % msg)
        self.__message_thread.send(msg)

    def parse_out_message(self, message):
        """
        Parse output or error messages from payload.

        :returns: a dict {'id': <id>, 'status': <status>, 'output': <output if produced>, 'cpu': <cpu>, 'wall': <wall>, 'message': <full message>}
        """

        logger.debug('parsing message: %s' % message)
        try:
            if message.startswith("/"):
                parts = message.split(",")
                ret = {'output': parts[0]}
                parts = parts[1:]
                for part in parts:
                    name, value = part.split(":")
                    name = name.lower()
                    ret[name] = value
                ret['status'] = 'finished'
                return ret
            elif message.startswith('ERR'):
                if "ERR_ATHENAMP_PARSE" in message:
                    pattern = re.compile(r"(ERR\_[A-Z\_]+)\ (.+)\:\ ?(.+)")
                    found = re.findall(pattern, message)
                    event_range = found[0][1]
                    if "eventRangeID" in event_range:
                        pattern = re.compile(r"eventRangeID\'\:\ ?.?\'([0-9\-]+)")
                        found = re.findall(pattern, event_range)
                        event_range_id = found[0]
                        ret = {'id': event_range_id, 'status': 'failed', 'message': message}
                        return ret
                    else:
                        raise Exception("Failed to parse %s" % message)
                else:
                    pattern = re.compile(r"(ERR\_[A-Z\_]+)\ ([0-9\-]+)\:\ ?(.+)")
                    found = re.findall(pattern, message)
                    event_range_id = found[0][1]
                    ret = {'id': event_range_id, 'status': 'failed', 'message': message}
                    return ret
            else:
                raise UnKnownException("Unknown message %s" % message)
        except PilotException as e:
            raise e
        except Exception as e:
            raise UnKnownException(e)

    def handle_out_message(self, message):
        """
        Handle output or error messages from payload.
        Messages from payload will be parsed and the handle_out_message hook is called.
        """

        logger.debug('handling out message: %s' % message)
        if not self.handle_out_message_hook:
            raise SetupFailure("handle_out_message_hook is not set")

        try:
            message_status = self.parse_out_message(message)
            logger.debug('parsed out message: %s' % message_status)
            logger.debug('calling handle_out_message hook(%s) to handle parsed message.' % self.handle_out_message_hook)
            self.handle_out_message_hook(message_status)
        except Exception as e:
            raise RunPayloadFailure("Failed to handle out message: %s" % e)

    def handle_messages(self):
        """
        Monitor the message queue to get output or error messages from payload and response to different messages.
        """

        try:
            message = self.__message_queue.get(False)
        except Queue.Empty:
            pass
        else:
            logger.debug('received message from payload: %s' % message)
            if "Ready for events" in message:
                event_ranges = self.get_event_ranges()
                if not event_ranges:
                    event_ranges = "No more events"
                self.send_event_ranges_to_payload(event_ranges)
            else:
                self.handle_out_message(message)

    def terminate(self, time_to_wait=30):
        """
        Terminate running threads and processes.
        """
        logger.info('terminate running threads and processes.')
        try:
            if self.__message_thread:
                self.__message_thread.stop()
                time.sleep(2)
            if self.__process:
                if not self.__process.poll() is None:
                    if self.__process.poll() == 0:
                        logger.info("payload finished successfully.")
                    else:
                        logger.error("payload finished with error code: %s" % self.__process.poll())
                else:
                    logger.info('killing payload process.')
                    pgid = os.getpgid(self.__process.pid)
                    logger.info('got process group id for pid %s: %s' % (self.__process.pid, pgid))
                    logger.info('send SIGTERM to process group: %s' % pgid)
                    os.killpg(pgid, signal.SIGTERM)
                    waiting_start = time.time()
                    while waiting_start > time.time() - time_to_wait:
                        if not self.__process.poll() is None:
                            break
                    if self.__process.poll() is None:
                        logger.info('process is still running. send SIGKILL to process group: %s' % pgid)
                        os.killpg(pgid, signal.SIGKILL)
        except Exception as e:
            logger.error('Exception caught when terminating ESProcess: %s' % e)
            raise UnKnownException(e)

    def run(self):
        """
        Main run loops.
        """

        logger.debug('initializing.')
        self.init()
        logger.debug('initialization finished.')

        logger.debug('starts to main loop')
        while True:
            try:
                self.monitor()
                self.handle_messages()
                time.sleep(1)
            except PilotException as e:
                logger.error('Exception caught in the main loop: %s' % e.get_detail())
                # TODO: define output message exception. If caught 3 output message exception, terminate
            except Exception as e:
                logger.error('Exception caught in the main loop: %s' % e)
                # TODO: catch and raise exceptions
                # if catching dead process exception, terminate.
                self.terminate()
                break
