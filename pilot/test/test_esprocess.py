# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

import json
import logging
import os
import subprocess
import sys
import threading
import time

try:
    import Queue as queue  # noqa: N813
except Exception:
    import queue  # Python 3

from pilot.eventservice.esprocess.eshook import ESHook
from pilot.eventservice.esprocess.esmanager import ESManager
from pilot.eventservice.esprocess.esmessage import MessageThread
from pilot.eventservice.esprocess.esprocess import ESProcess

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def check_env():
    """
    Function to check whether cvmfs is available.
    To be used to decide whether to skip some test functions.

    :returns True: if cvmfs is available. Otherwise False.
    """
    return os.path.exists('/cvmfs/atlas.cern.ch/repo/')


class TestESHook(ESHook):
    """
    A class implemented ESHook, to be used to test eventservice.
    """

    def __init__(self):
        """
        Init the hook class for tests: Read payload and event ranges from a file.
                                       Download evgen files which are needed to run payload.
        """
        with open('pilot/test/resource/eventservice_job.txt') as job_file:
            job = json.load(job_file)
            self.__payload = job['payload']
            self.__event_ranges = job['event_ranges']  # doesn't exit

        if check_env():
            process = subprocess.Popen('pilot/test/resource/download_test_es_evgen.sh', shell=True, stdout=subprocess.PIPE)
            process.wait()
            if process.returncode != 0:
                raise Exception('failed to download input files for es test: %s %s' % (process.communicate()))
        else:
            logging.info("No CVMFS. skip downloading files.")

        self.__injected_event_ranges = []
        self.__outputs = []

    def get_payload(self):
        """
        Get payload hook function for tests.

        :returns: dict {'executable': <cmd string>, 'output_file': <filename or without it>, 'error_file': <filename or without it>}
        """

        return self.__payload

    def get_event_ranges(self, num_ranges=1):
        """
        Get event ranges hook function for tests.

        :returns: dict of event ranges.
                  None if no available events.
        """
        ret = []
        for _ in range(num_ranges):
            if len(self.__event_ranges) > 0:
                event_range = self.__event_ranges.pop(0)
                ret.append(event_range)
                self.__injected_event_ranges.append(event_range)
        return ret

    def handle_out_message(self, message):
        """
        Handle ES output or error messages hook function for tests.

        :param message: a dict of parsed message.
                        For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                        Fro 'failed' event ranges, it's {'id': <id>, 'status': 'failed', 'message': <full message>}.
        """

        print(message)
        self.__outputs.append(message)

    def get_injected_event_ranges(self):
        """
        Get event ranges injected to payload for test assertion.

        :returns: List of injected event ranges.
        """
        return self.__injected_event_ranges

    def get_outputs(self):
        """
        Get outputs for test assertion.

        :returns: List of outputs.
        """
        return self.__outputs


class TestESMessageThread(unittest.TestCase):
    """
    Unit tests for event service message thread.
    """

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_msg_thread(self):
        """
        Make sure that es message thread works as expected.
        """
        _queue = queue.Queue()  # Python 2/3
        msg_thread = MessageThread(_queue, socket_name='test', context='local')
        self.assertIsInstance(msg_thread, threading.Thread)

        msg_thread.start()
        time.sleep(1)
        self.assertTrue(msg_thread.is_alive())

        msg_thread.send('test')
        msg_thread.stop()
        self.assertTrue(msg_thread.is_stopped())
        time.sleep(1)
        self.assertFalse(msg_thread.is_alive())


@unittest.skipIf(not check_env(), "No CVMFS")
class TestESProcess(unittest.TestCase):
    """
    Unit tests for event service process functions
    """

    @classmethod
    def setUpClass(cls):
        cls._test_hook = TestESHook()
        cls._esProcess = ESProcess(cls._test_hook.get_payload())

    def test_set_get_event_ranges_hook(self):
        """
        Make sure that no exceptions to set get_event_ranges hook.
        """

        self._esProcess.set_get_event_ranges_hook(self._test_hook.get_event_ranges)
        self.assertEqual(self._test_hook.get_event_ranges, self._esProcess.get_get_event_ranges_hook())

    def test_set_handle_out_message_hook(self):
        """
        Make sure that no exceptions to set handle_out_message hook.
        """

        self._esProcess.set_handle_out_message_hook(self._test_hook.handle_out_message)
        self.assertEqual(self._test_hook.handle_out_message, self._esProcess.get_handle_out_message_hook())

    def test_parse_out_message(self):
        """
        Make sure to parse messages from payload correctly.
        """

        output_msg = '/tmp/HITS.12164365._000300.pool.root.1.12164365-3616045203-10980024041-4138-8,ID:12164365-3616045203-10980024041-4138-8,CPU:288,WALL:303'
        ret = self._esProcess.parse_out_message(output_msg)
        self.assertEqual(ret['status'], 'finished')
        self.assertEqual(ret['id'], '12164365-3616045203-10980024041-4138-8')

        error_msg1 = 'ERR_ATHENAMP_PROCESS 130-2068634812-21368-1-4: Failed to process event range'
        ret = self._esProcess.parse_out_message(error_msg1)
        self.assertEqual(ret['status'], 'failed')
        self.assertEqual(ret['id'], '130-2068634812-21368-1-4')

        error_msg2 = "ERR_ATHENAMP_PARSE \"u'LFN': u'eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-4', u'startEvent': 5\": Wrong format"
        ret = self._esProcess.parse_out_message(error_msg2)
        self.assertEqual(ret['status'], 'failed')
        self.assertEqual(ret['id'], '130-2068634812-21368-1-4')


class TestEventService(unittest.TestCase):
    """
    Unit tests for event service functions.
    """

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_init_esmanager(self):
        """
        Make sure that no exceptions to init ESManager
        """
        test_hook = TestESHook()
        es_manager = ESManager(test_hook)
        self.assertIsInstance(es_manager, ESManager)

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_run_es(self):
        """
        Make sure that ES produced all events that injected.
        """
        test_hook = TestESHook()
        es_manager = ESManager(test_hook)
        es_manager.run()
        injected_event = test_hook.get_injected_event_ranges()
        outputs = test_hook.get_outputs()

        self.assertEqual(len(injected_event), len(outputs))
        self.assertNotEqual(len(outputs), 0)
