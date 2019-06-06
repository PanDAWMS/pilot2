#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors: Pavlo Svirin <pavlo.svirin@gmail.com>

import unittest
import os

# from pilot.control.job import get_fake_job
# from pilot.info import JobData
from pilot.info.filespec import FileSpec
from pilot.util.tracereport import TraceReport


def check_env():
    """
    Function to check whether rucio copytool is loaded correctly.
    To be used to decide whether to skip some test functions.
    :returns True: if rucio copytool is available. Otherwise False.
    """
    aval = False
    return aval


@unittest.skipIf(not check_env(), "No Rucio copytool")
class TestCopytoolRucio(unittest.TestCase):
    """
    Unit tests for rucio copytool.
    """

    def setUp(self):
        test_file = open('test.txt', 'w')
        test_file.write('For test purposes only.')
        test_file.close()
        fspec_out = FileSpec()
        fspec_out.lfn = 'test.txt'
        fspec_out.scope = 'user.tjavurek'
        fspec_out.checksum = {'adler32': '682c08b9'}
        fspec_out.pfn = os.getcwd() + '/' + 'test.txt'
        fspec_out.ddmendpoint = 'UNI-FREIBURG_SCRATCHDISK'
        self.outdata = [fspec_out]

    def test_copy_out_rucio(self):
        from pilot.copytool.rucio import copy_out
        trace_report = TraceReport()
        trace_report.update(eventType='unit test')
        copy_out(self.outdata, trace_report=trace_report)
        os.remove(self.outdata[0].pfn)


if __name__ == '__main__':
    unittest.main()
