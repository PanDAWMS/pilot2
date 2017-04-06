# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017

import os
import shutil
import tempfile
import unittest

from pilot.api import data


class TestHarvester(unittest.TestCase):
    '''
    Automatic stage-in tests for Harvester.

        from pilot.api import data
        data_client = data.StageInClient(site)
        result = data_client.transfer(files=[{scope, name, destination}, ...])

    Notabene:
      The following datasets with their constituent files are replicated
      on every DATADISK and should thus be generally available:

        user.mlassnig:user.mlassnig.pilot.test.single.hits
          mc15_13TeV:HITS.06828093._000096.pool.root.1

        user.mlassnig:user.mlassnig.pilot.test.multi.hits
          mc15_14TeV:HITS.10075481._000432.pool.root.1
          mc15_14TeV:HITS.10075481._000433.pool.root.1
          mc15_14TeV:HITS.10075481._000434.pool.root.1
          mc15_14TeV:HITS.10075481._000435.pool.root.1
          mc15_14TeV:HITS.10075481._000444.pool.root.1
          mc15_14TeV:HITS.10075481._000445.pool.root.1
          mc15_14TeV:HITS.10075481._000451.pool.root.1
          mc15_14TeV:HITS.10075481._000454.pool.root.1
          mc15_14TeV:HITS.10075481._000455.pool.root.1
    '''

    def setUp(self):
        # skip tests if running through Travis -- github does not have working rucio
        self.travis = False
        if os.environ.get('TRAVIS') == 'true':
            self.travis = True

        # setup pilot data client
        self.data_client = data.StageInClient(site='CERN-PROD')

    def test_stagein_sync_simple(self):
        '''
        Single file going to a destination directory.
        '''
        if self.travis:
            return True

        result = self.data_client.transfer(files=[{'scope': 'mc15_13TeV',
                                                   'name': 'HITS.06828093._000096.pool.root.1',
                                                   'destination': '/tmp'}])

        os.remove('/tmp/HITS.06828093._000096.pool.root.1')

        for file in result:
            self.assertEqual(file['errno'], 0)

    def test_stagein_sync_merged_same(self):
        '''
        Multiple files going to the same destination directory.
        '''
        if self.travis:
            return True

        result = self.data_client.transfer(files=[{'scope': 'mc15_14TeV',
                                                   'name': 'HITS.10075481._000432.pool.root.1',
                                                   'destination': '/tmp'},
                                                  {'scope': 'mc15_14TeV',
                                                   'name': 'HITS.10075481._000433.pool.root.1',
                                                   'destination': '/tmp'}])

        os.remove('/tmp/HITS.10075481._000432.pool.root.1')
        os.remove('/tmp/HITS.10075481._000433.pool.root.1')

        for file in result:
            self.assertEqual(file['errno'], 0)

    def test_stagein_sync_merged_diff(self):
        '''
        Multiple files going to different destination directories.
        '''
        if self.travis:
            return True

        tmp_dir1, tmp_dir2 = tempfile.mkdtemp(), tempfile.mkdtemp()
        result = self.data_client.transfer(files=[{'scope': 'mc15_14TeV',
                                                   'name': 'HITS.10075481._000432.pool.root.1',
                                                   'destination': tmp_dir1},
                                                  {'scope': 'mc15_14TeV',
                                                   'name': 'HITS.10075481._000433.pool.root.1',
                                                   'destination': tmp_dir2}])

        shutil.rmtree(tmp_dir1)
        shutil.rmtree(tmp_dir2)

        for file in result:
            self.assertEqual(file['errno'], 0)
