#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors: Pavlo Svirin <pavlo.svirin@gmail.com>

import unittest
import string
import tempfile
import random
import os.path

from pilot.copytool.rucio import copy_in
from pilot.util.container import execute

# from pilot.control.job import get_fake_job
# from pilot.info import JobData
from pilot.info.filespec import FileSpec


class TestCopytoolRucio(unittest.TestCase):
    """
    Unit tests for rucio copytool.
    """

    def setUp(self):

        data = {'did_scope': 'mc16_13TeV',
                'did': 'mc16_13TeV:EVNT.16337107._000147.pool.root.1',
                'rse': 'IFIC-LCG2_DATADISK',
                'pfn': 'root://t2fax.ific.uv.es:1094//lustre/ific.uv.es/grid/atlas/atlasdatadisk/rucio/mc16_13TeV/59/29/EVNT.16337107._000147.pool.root.1',
                'did_name': 'EVNT.16337107._000147.pool.root.1',
                'dataset': 'mc16_13TeV:mc16_13TeV.410431.PhPy8EG_A14_ttbar_hdamp517p5_allhad_HT1k_1k5.merge.EVNT.e6735_e5984_tid16337107_00',
                'filesize': 490891918,
                'guid': '1048c73e4cf60442baf3be58fb711928',
                'checksum': '3684d24c',
                'base_dir': '/var/lib/torque/tmpdir/76952314.ce05.ific.uv.es/condorg_QFWCLP24/Panda_Pilot_19263_1544445888/PandaJob'}
        fspec = FileSpec()
        fspec.lfn = data['did_name']
        fspec.scope = data['did_scope']
        fspec.did = data['did']
        fspec.ddmendpoint = data['rse']
        fspec.dataset = data['dataset']
        fspec.turl = data['pfn']
        fspec.filesize = data['filesize']
        fspec.guid = data['guid']
        fspec.checksum = data['checksum']
        fspec.no_subdir = False
        # fspec.is_directaccess = False

        self.indata = [fspec]
        # for f in self.indata:
        #     f.workdir = self.tmp_dst_dir
        #     f.turl = os.path.join(self.tmp_src_dir, f.lfn)

        self.outdata = []  # jdata.prepare_outfiles(data)

    def test_copy_in_rucio(self):
        copy_in(self.indata, trace_report={'eventType': 'unit test'})


if __name__ == '__main__':
    unittest.main()
