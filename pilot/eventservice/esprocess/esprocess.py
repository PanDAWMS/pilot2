# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

import io
import json
import logging
import os
import re
import subprocess
import time
import threading
import traceback

try:
    import Queue as queue  # noqa: N813
except Exception:
    import queue  # Python 3

from pilot.common.exception import PilotException, MessageFailure, SetupFailure, RunPayloadFailure, UnknownException
from pilot.eventservice.esprocess.esmessage import MessageThread
from pilot.util.container import containerise_executable
from pilot.util.processes import kill_child_processes

logger = logging.getLogger(__name__)

"""
Main process to handle event service.
It makes use of two hooks get_event_ranges_hook and handle_out_message_hook to communicate with other processes when
it's running. The process will handle the logic of Event service independently.
"""


class ESProcess(threading.Thread):
    """
    Main EventService Process.
    """
    def __init__(self, payload, waiting_time=30 * 60):
        """
        Init ESProcess.

        :param payload: a dict of {'executable': <cmd string>, 'output_file': <filename or without it>, 'error_file': <filename or without it>}
        """
        threading.Thread.__init__(self, name='esprocess')

        self.__message_queue = queue.Queue()
        self.__payload = payload

        self.__message_thread = None
        self.__process = None

        self.get_event_ranges_hook = None
        self.handle_out_message_hook = None

        self.__monitor_log_time = None
        self.is_no_more_events = False
        self.__no_more_event_time = None
        self.__waiting_time = waiting_time
        self.__stop = threading.Event()
        self.__stop_time = 180
        self.pid = None
        self.__is_payload_started = False

        self.__ret_code = None
        self.setName("ESProcess")
        self.corecount = 1

        self.event_ranges_cache = []

    def __del__(self):
        if self.__message_thread:
            self.__message_thread.stop()

    def is_payload_started(self):
        return self.__is_payload_started

    def stop(self, delay=1800):
        if not self.__stop.is_set():
            self.__stop.set()
            self.__stop_set_time = time.time()
            self.__stop_delay = delay
            event_ranges = "No more events"
            self.send_event_ranges_to_payload(event_ranges)

    def init_message_thread(self, socketname=None, context='local'):
        """
        init message thread.

        :param socket_name: name of the socket between current process and payload.
        :param context: name of the context between current process and payload, default is 'local'.

        :raises MessageFailure: when failed to init message thread.
        """

        logger.info("start to init message thread")
        try:
            self.__message_thread = MessageThread(self.__message_queue, socketname, context)
            self.__message_thread.start()
        except PilotException as e:
            logger.error("Failed to start message thread: %s" % e.get_detail())
            self.__ret_code = -1
        except Exception as e:
            logger.error("Failed to start message thread: %s" % str(e))
            self.__ret_code = -1
            raise MessageFailure(e)
        logger.info("finished to init message thread")

    def stop_message_thread(self):
        """
        Stop message thread
        """
        logger.info("Stopping message thread")
        if self.__message_thread:
            while self.__message_thread.is_alive():
                if not self.__message_thread.is_stopped():
                    self.__message_thread.stop()
        logger.info("Message thread stopped")

    def init_yampl_socket(self, executable):
        socket_name = self.__message_thread.get_yampl_socket_name()
        if "PILOT_EVENTRANGECHANNEL" in executable:
            executable = "export PILOT_EVENTRANGECHANNEL=\"%s\"; " % (socket_name) + executable
        elif "--preExec" not in executable:
            executable += " --preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\'" % (socket_name)
        else:
            if "import jobproperties as jps" in executable:
                executable = executable.replace("import jobproperties as jps;",
                                                "import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\";" % (socket_name))
            else:
                if "--preExec " in executable:
                    new_str = "--preExec \'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"%s\"\' " % socket_name
                    executable = executable.replace("--preExec ", new_str)
                else:
                    logger.warn("--preExec has an unknown format - expected \'--preExec \"\' or \"--preExec \'\", got: %s" % (executable))

        return executable

    def init_payload_process(self):
        """
        init payload process.

        :raise SetupFailure: when failed to init payload process.
        """

        logger.info("start to init payload process")
        try:
            try:
                workdir = self.get_workdir()
            except Exception as e:
                raise e

            executable = self.get_executable(workdir)
            output_file_fd = self.get_file(workdir, file_label='output_file', file_name='ES_payload_output.txt')
            error_file_fd = self.get_file(workdir, file_label='error_file', file_name='ES_payload_error.txt')

            # containerise executable if required
            if 'job' in self.__payload and self.__payload['job']:
                try:
                    executable, diagnostics = containerise_executable(executable, job=self.__payload['job'], workdir=workdir)
                    if diagnostics:
                        msg = 'containerisation of executable failed: %s' % diagnostics
                        logger.warning(msg)
                        raise SetupFailure(msg)
                except Exception as e:
                    msg = 'exception caught while preparing container command: %s' % e
                    logger.warning(msg)
                    raise SetupFailure(msg)
            else:
                logger.warning('could not containerise executable')

            # get the process
            self.__process = subprocess.Popen(executable, stdout=output_file_fd, stderr=error_file_fd, shell=True)
            self.pid = self.__process.pid
            self.__is_payload_started = True
            logger.debug("Started new processs (executable: %s, stdout: %s, stderr: %s, pid: %s)" % (executable,
                                                                                                     output_file_fd,
                                                                                                     error_file_fd,
                                                                                                     self.__process.pid))
            if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].corecount:
                self.corecount = int(self.__payload['job'].corecount)
        except PilotException as e:
            logger.error("Failed to start payload process: %s, %s" % (e.get_detail(), traceback.format_exc()))
            self.__ret_code = -1
        except Exception as e:
            logger.error("Failed to start payload process: %s, %s" % (str(e), traceback.format_exc()))
            self.__ret_code = -1
            raise SetupFailure(e)
        logger.info("finished initializing payload process")

    def get_file(self, workdir, file_label='output_file', file_name='ES_payload_output.txt'):
        """
        Return the requested file.

        :param file_label:
        :param workdir:
        :return:
        """

        try:
            file_type = file  # Python 2
        except NameError:
            file_type = io.IOBase  # Python 3

        if file_label in self.__payload:
            if isinstance(self.__payload[file_label], file_type):
                _file_fd = self.__payload[file_label]
            else:
                _file = self.__payload[file_label] if '/' in self.__payload[file_label] else os.path.join(workdir, self.__payload[file_label])
                _file_fd = open(_file, 'w')
        else:
            _file = os.path.join(workdir, file_name)
            _file_fd = open(_file, 'w')

        return _file_fd

    def get_workdir(self):
        """
        Return the workdir.
        If the workdir is set but is not a directory, return None.

        :return: workdir (string or None).
        :raises SetupFailure: in case workdir is not a directory.
        """

        workdir = ''
        if 'workdir' in self.__payload:
            workdir = self.__payload['workdir']
            if not os.path.exists(workdir):
                os.makedirs(workdir)
            elif not os.path.isdir(workdir):
                raise SetupFailure('workdir exists but is not a directory')
        return workdir

    def get_executable(self, workdir):
        """
        Return the executable string.

        :param workdir: work directory (string).
        :return: executable (string).
        """
        executable = self.__payload['executable']
        executable = self.init_yampl_socket(executable)
        return 'cd %s; %s' % (workdir, executable)

    def set_get_event_ranges_hook(self, hook):
        """
        set get_event_ranges hook.

        :param hook: a hook method to get event ranges.
        """

        self.get_event_ranges_hook = hook

    def get_get_event_ranges_hook(self):
        """
        get get_event_ranges hook.

        :returns: The hook method to get event ranges.
        """

        return self.get_event_ranges_hook

    def set_handle_out_message_hook(self, hook):
        """
        set handle_out_message hook.

        :param hook: a hook method to handle payload output and error messages.
        """

        self.handle_out_message_hook = hook

    def get_handle_out_message_hook(self):
        """
        get handle_out_message hook.

        :returns: The hook method to handle payload output and error messages.
        """

        return self.handle_out_message_hook

    def init(self):
        """
        initialize message thread and payload process.
        """

        try:
            self.init_message_thread()
            self.init_payload_process()
        except Exception as e:
            # TODO: raise exceptions
            self.__ret_code = -1
            self.stop()
            raise e

    def monitor(self):
        """
        Monitor whether a process is dead.

        raises: MessageFailure: when the message thread is dead or exited.
                RunPayloadFailure: when the payload process is dead or exited.
        """

        if self.__no_more_event_time and time.time() - self.__no_more_event_time > self.__waiting_time:
            self.__ret_code = -1
            raise Exception('Too long time (%s seconds) since "No more events" is injected' %
                            (time.time() - self.__no_more_event_time))

        if self.__monitor_log_time is None or self.__monitor_log_time < time.time() - 10 * 60:
            self.__monitor_log_time = time.time()
            logger.info('monitor is checking dead process.')

        if self.__message_thread is None:
            raise MessageFailure("Message thread has not started.")
        if not self.__message_thread.is_alive():
            raise MessageFailure("Message thread is not alive.")

        if self.__process is None:
            raise RunPayloadFailure("Payload process has not started.")
        if self.__process.poll() is not None:
            if self.is_no_more_events:
                logger.info("Payload finished with no more events")
            else:
                self.__ret_code = self.__process.poll()
                raise RunPayloadFailure("Payload process is not alive: %s" % self.__process.poll())

        if self.__stop.is_set() and time.time() > self.__stop_set_time + self.__stop_delay:
            logger.info("Stop has been set for %s seconds, which is more than the stop wait time. Will terminate" % self.__stop_delay)
            self.terminate()

    def has_running_children(self):
        """
        Check whether it has running children

        :return: True if there are alive children, otherwise False
        """
        if self.__message_thread and self.__message_thread.is_alive():
            return True
        if self.__process and self.__process.poll() is None:
            return True
        return False

    def is_payload_running(self):
        """
        Check whether the payload is still running

        :return: True if the payload is running, otherwise False
        """
        if self.__process and self.__process.poll() is None:
            return True
        return False

    def get_event_range_to_payload(self):
        """
        Get one event range to be sent to payload
        """
        logger.debug("Number of cached event ranges: %s" % len(self.event_ranges_cache))
        if not self.event_ranges_cache:
            event_ranges = self.get_event_ranges()
            if event_ranges:
                self.event_ranges_cache.extend(event_ranges)

        if self.event_ranges_cache:
            event_range = self.event_ranges_cache.pop(0)
            return event_range
        else:
            return []

    def get_event_ranges(self, num_ranges=None):
        """
        Calling get_event_ranges hook to get event ranges.

        :param num_ranges: number of event ranges to get.

        :raises: SetupFailure: If get_event_ranges_hook is not set.
                 MessageFailure: when failed to get event ranges.
        """
        if not num_ranges:
            num_ranges = self.corecount

        logger.debug('getting event ranges(num_ranges=%s)' % num_ranges)
        if not self.get_event_ranges_hook:
            raise SetupFailure("get_event_ranges_hook is not set")

        try:
            logger.debug('calling get_event_ranges hook(%s) to get event ranges.' % self.get_event_ranges_hook)
            event_ranges = self.get_event_ranges_hook(num_ranges)
            logger.debug('got event ranges: %s' % event_ranges)
            return event_ranges
        except Exception as e:
            raise MessageFailure("Failed to get event ranges: %s" % e)

    def send_event_ranges_to_payload(self, event_ranges):
        """
        Send event ranges to payload through message thread.

        :param event_ranges: list of event ranges.
        """

        msg = None
        if "No more events" in event_ranges:
            msg = event_ranges
            self.is_no_more_events = True
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

        :param message: The message string received from payload.

        :returns: a dict {'id': <id>, 'status': <status>, 'output': <output if produced>, 'cpu': <cpu>, 'wall': <wall>, 'message': <full message>}
        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
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
                raise UnknownException("Unknown message %s" % message)
        except PilotException as e:
            raise e
        except Exception as e:
            raise UnknownException(e)

    def handle_out_message(self, message):
        """
        Handle output or error messages from payload.
        Messages from payload will be parsed and the handle_out_message hook is called.

        :param message: The message string received from payload.

        :raises: SetupFailure: when handle_out_message_hook is not set.
                 RunPayloadFailure: when failed to handle an output or error message.
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
        except queue.Empty:
            pass
        else:
            logger.debug('received message from payload: %s' % message)
            if "Ready for events" in message:
                event_ranges = self.get_event_range_to_payload()
                if not event_ranges:
                    event_ranges = "No more events"
                self.send_event_ranges_to_payload(event_ranges)
            else:
                self.handle_out_message(message)

    def poll(self):
        """
        poll whether the process is still running.

        :returns: None: still running.
                  0: finished successfully.
                  others: failed.
        """
        return self.__ret_code

    def terminate(self, time_to_wait=1):
        """
        Terminate running threads and processes.

        :param time_to_wait: integer, seconds to wait to force kill the payload process.

        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """
        logger.info('terminate running threads and processes.')
        try:
            self.stop()
            if self.__process:
                if not self.__process.poll() is None:
                    if self.__process.poll() == 0:
                        logger.info("payload finished successfully.")
                    else:
                        logger.error("payload finished with error code: %s" % self.__process.poll())
                else:
                    for i in range(time_to_wait * 10):
                        if not self.__process.poll() is None:
                            break
                        time.sleep(1)

                    if not self.__process.poll() is None:
                        if self.__process.poll() == 0:
                            logger.info("payload finished successfully.")
                        else:
                            logger.error("payload finished with error code: %s" % self.__process.poll())
                    else:
                        logger.info('terminating payload process.')
                        pgid = os.getpgid(self.__process.pid)
                        logger.info('got process group id for pid %s: %s' % (self.__process.pid, pgid))
                        # logger.info('send SIGTERM to process group: %s' % pgid)
                        # os.killpg(pgid, signal.SIGTERM)
                        logger.info('send SIGTERM to process: %s' % self.__process.pid)
                        kill_child_processes(self.__process.pid)
                self.__ret_code = self.__process.poll()
            else:
                self.__ret_code = -1
        except Exception as e:
            logger.error('Exception caught when terminating ESProcess: %s' % e)
            self.__ret_code = -1
            self.stop()
            raise UnknownException(e)

    def kill(self):
        """
        Terminate running threads and processes.

        :param time_to_wait: integer, seconds to wait to force kill the payload process.

        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """
        logger.info('terminate running threads and processes.')
        try:
            self.stop()
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
                    # logger.info('send SIGKILL to process group: %s' % pgid)
                    # os.killpg(pgid, signal.SIGKILL)
                    logger.info('send SIGKILL to process: %s' % self.__process.pid)
                    kill_child_processes(self.__process.pid)
        except Exception as e:
            logger.error('Exception caught when terminating ESProcess: %s' % e)
            self.stop()
            raise UnknownException(e)

    def clean(self):
        """
        Clean left resources
        """
        self.terminate()

    def run(self):
        """
        Main run loops: monitor message thread and payload process.
                        handle messages from payload and response messages with injecting new event ranges or process outputs.

        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """

        logger.info('start esprocess with thread ident: %s' % (self.ident))
        logger.debug('initializing')
        self.init()
        logger.debug('initialization finished.')

        logger.info('starts to main loop')
        while self.is_payload_running():
            try:
                self.monitor()
                self.handle_messages()
                time.sleep(0.01)
            except PilotException as e:
                logger.error('PilotException caught in the main loop: %s, %s' % (e.get_detail(), traceback.format_exc()))
                # TODO: define output message exception. If caught 3 output message exception, terminate
                self.stop()
            except Exception as e:
                logger.error('Exception caught in the main loop: %s, %s' % (e, traceback.format_exc()))
                # TODO: catch and raise exceptions
                # if catching dead process exception, terminate.
                self.stop()
                break
        self.clean()
        self.stop_message_thread()
        logger.debug('main loop finished')
