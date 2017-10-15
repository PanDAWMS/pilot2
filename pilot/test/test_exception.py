#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017


import logging
import sys
import unittest

from pilot.common.exception import RunPayloadFailure, PilotException

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


class TestException(unittest.TestCase):
    """
    Unit tests for exceptions.
    """

    def test_run_payload_failure(self):
        """
        Make sure that es message thread works as expected.
        """

        try:
            pass
            raise RunPayloadFailure(a='message a', b='message b')
        except PilotException as ex:
            self.assertIsInstance(ex, PilotException)
            self.assertEqual(ex.get_error_code(), 1111)
            logging.info("\nException: error code: %s\n\nMain message: %s\n\nFullStack: %s" % (ex.get_error_code(), str(ex), ex.get_detail()))

        try:
            pass
            raise RunPayloadFailure("Test message")
        except PilotException as ex:
            self.assertIsInstance(ex, PilotException)
            self.assertEqual(ex.get_error_code(), 1111)
            logging.info("\nException: error code: %s\n\nMain message: %s\n\nFullStack: %s" % (ex.get_error_code(), str(ex), ex.get_detail()))
