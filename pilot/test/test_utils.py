#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import unittest
import os

from pilot.util.workernode import collect_workernode_info, get_disk_space


class TestUtils(unittest.TestCase):
    """
    Unit tests for utils functions.
    """

    def setUp(self):
        # skip tests if running on a Mac -- Macs don't have /proc
        self.mac = False
        if os.environ.get('MACOSX') == 'true' or not os.path.exists('/proc/meminfo'):
            self.mac = True

        from pilot.info import infosys
        infosys.init("CERN")

    def test_collect_workernode_info(self):
        """
        Make sure that collect_workernode_info() returns the proper types (float, float, float).

        :return: (assertion)
        """

        if self.mac:
            return True

        mem, cpu, disk = collect_workernode_info(path=os.getcwd())

        self.assertEqual(type(mem), float)
        self.assertEqual(type(cpu), float)
        self.assertEqual(type(disk), float)

        self.assertNotEqual(mem, 0.0)
        self.assertNotEqual(cpu, 0.0)
        self.assertNotEqual(disk, 0.0)

    def test_get_disk_space(self):
        """
        Verify that get_disk_space() returns the proper type (int).

        :return: (assertion)
        """

        if self.mac:
            return True

        #queuedata = {'maxwdir': 123456789}
        from pilot.info import infosys

        diskspace = get_disk_space(infosys.queuedata)  ## FIX ME LATER

        self.assertEqual(type(diskspace), int)


if __name__ == '__main__':
    unittest.main()
