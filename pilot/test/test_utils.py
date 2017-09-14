#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import unittest

import pilot.util

class TestUtils(unittest.TestCase):
    """
    Unit tests for utils functions.
    """

    def test_collect_workernode_info(self):
        """
        Make sure that collect_workernode_info() returns the proper types (float, float).

        :return: (assertion)
        """

        mem, cpu = pilot.util.collect_workernode_info()

        self.assertEqual(type(mem), float)
        self.assertEqual(type(cpu), float)

        self.assertNotEqual(mem, 0.0)
        self.assertNotEqual(cpu, 0.0)
