#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import unittest

from pilot.util.workernode import collect_workernode_info, get_disk_space


class TestUtils(unittest.TestCase):
    """
    Unit tests for utils functions.
    """

    def setUp(self):
        # skip tests if running on a Mac -- Macs don't have /proc
        self.mac = False
        if os.environ.get('MACOSX') == 'true':
            self.mac = True

    def test_collect_workernode_info(self):
        """
        Make sure that collect_workernode_info() returns the proper types (float, float).

        :return: (assertion)
        """

        if self.mac:
            return True

        mem, cpu = collect_workernode_info()

        self.assertEqual(type(mem), float)
        self.assertEqual(type(cpu), float)

        self.assertNotEqual(mem, 0.0)
        self.assertNotEqual(cpu, 0.0)

    def test_get_disk_space(self):
        """
        Verify that get_disk_space() returns the proper type (int).

        :return: (assertion)
        """

        if self.mac:
            return True

        queuedata = {'maxwdir': 123456789}
        diskspace = get_disk_space(queuedata)

        self.assertEqual(type(diskspace), int)
