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

from pilot.api import analytics


class TestAnalytics(unittest.TestCase):
    """
    Unit tests for the Analytics package.
    """

    def setUp(self):

        self.client = analytics.Analytics()

    def test_linear_fit(self):
        """
        Make sure that a linear fit works.

        :return: (assertion).
        """

        self.assertIsInstance(self.client, analytics.Analytics)  # python 2.7

        x = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        y = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        fit = self.client.fit(x, y)
        slope = fit.slope()
        intersect = fit.intersect()

        self.assertEqual(type(slope), float)
        self.assertEqual(slope, 1.0)
        self.assertEqual(type(intersect), float)
        self.assertEqual(intersect, 0.0)

        y = [0, -1, -2, -3, -4, -5, -6, -7, -8, -9]

        fit = self.client.fit(x, y)
        slope = fit.slope()

        self.assertEqual(slope, -1.0)

    def test_parsing_memory_monitor_data(self):
        """
        Read and fit PSS vs Time from memory monitor output file.

        :return: (assertion).
        """

        # old MemoryMonitor format
        filename = 'pilot/test/resource/memory_monitor_output.txt'
        self.assertEqual(os.path.exists(filename), True)

        table = self.client.get_table(filename)

        self.assertEqual(type(table), dict)

        x = table['Time']
        y = table['PSS']  # old MemoryMonitor format
        fit = self.client.fit(x, y)

        slope = fit.slope()

        self.assertEqual(type(slope), float)
        self.assertGreater(slope, 0)


if __name__ == '__main__':
    unittest.main()
