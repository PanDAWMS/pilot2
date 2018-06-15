#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import unittest

from pilot.api import analytics


class TestAnaytics(unittest.TestCase):
    """
    Unit tests for the Analytics package.
    """

    def setUp(self):

        self.client = analytics.Analytics()

    def test_linear_fit(self):
        """
        Make sure that a linear fit works.

        :return: (assertion)
        """

        x = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        y = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        fit = client.fit(x, y)
        slope = fit.slope()
        intersect = fit.intersect()

        self.assertEqual(type(slope), float)
        self.assertEqual(slope, 1.0)
        self.assertEqual(type(intersect), float)
        self.assertEqual(intersect, 0.0)

        y = [0, -1, -2, -3, -4, -5, -6, -7, -8, -9]

        fit = client.fit(x, y)
        slope = fit.slope()

        self.assertEqual(slope, -1.0)


if __name__ == '__main__':
    unittest.main()
