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

    def test_stagein_sync_fail_nodirectory(self):
        '''
        Test error message propagation
        '''
        if self.travis:
            return True

        result = self.data_client.transfer(files=[{'scope': 'does_not_matter',
                                                   'name': 'does_not_matter',
                                                   'destination': '/i_do_not_exist'},
                                                  {'scope': 'does_not_matter_too',
                                                   'name': 'does_not_matter_too',
                                                   'destination': '/neither_do_i'}])

        for file in result:
            self.assertEqual(file['errno'], 1)
            self.assertEqual(file['status'], 'failed')
            self.assertIn(file['errmsg'], ['Destination directory does not exist: /i_do_not_exist',
                                           'Destination directory does not exist: /neither_do_i'])

    def test_stagein_sync_fail_noexist(self):
        '''
        Test error message propagation
        '''
        if self.travis:
            return True

        result = self.data_client.transfer(files=[{'scope': 'no_scope1',
                                                   'name': 'no_name1',
                                                   'destination': '/tmp'},
                                                  {'scope': 'no_scope2',
                                                   'name': 'no_name2',
                                                   'destination': '/tmp'}])

        for file in result:
            self.assertEqual(file['errno'], 3)
            self.assertEqual(file['status'], 'failed')
            self.assertIn(file['errmsg'], ['Data identifier \'no_scope1:no_name1\' not found',
                                           'Data identifier \'no_scope2:no_name2\' not found'])

    def test_stagein_sync_fail_mix(self):
        '''
        Test error message propagation
        '''
        if self.travis:
            return True

        tmp_dir1, tmp_dir2 = tempfile.mkdtemp(), tempfile.mkdtemp()
        result = self.data_client.transfer(files=[{'scope': 'no_scope1',
                                                   'name': 'no_name1',
                                                   'destination': '/tmp'},
                                                  {'scope': 'mc15_13TeV',
                                                   'name': 'HITS.06828093._000096.pool.root.1',
                                                   'destination': tmp_dir1},
                                                  {'scope': 'mc15_13TeV',
                                                   'name': 'HITS.06828093._000096.pool.root.1',
                                                   'destination': tmp_dir2},
                                                  {'scope': 'no_scope2',
                                                   'name': 'no_name2',
                                                   'destination': '/tmp'}])
        ls_tmp_dir1 = os.listdir(tmp_dir1)
        ls_tmp_dir2 = os.listdir(tmp_dir2)
        shutil.rmtree(tmp_dir1)
        shutil.rmtree(tmp_dir2)
        self.assertIn('HITS.06828093._000096.pool.root.1', ls_tmp_dir1)
        self.assertIn('HITS.06828093._000096.pool.root.1', ls_tmp_dir2)

        for file in result:
            if file['name'] in ['no_name1', 'no_name2']:
                self.assertEqual(file['errno'], 3)
                self.assertEqual(file['status'], 'failed')
                self.assertIn(file['errmsg'], ['Data identifier \'no_scope1:no_name1\' not found',
                                               'Data identifier \'no_scope2:no_name2\' not found'])
            else:
                self.assertEqual(file['errno'], 0)
                self.assertEqual(file['status'], 'done')

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

        ls_tmp_dir1 = os.listdir(tmp_dir1)
        ls_tmp_dir2 = os.listdir(tmp_dir2)
        shutil.rmtree(tmp_dir1)
        shutil.rmtree(tmp_dir2)

        for file in result:
            self.assertEqual(file['errno'], 0)
            self.assertIn('HITS.10075481._000432.pool.root.1', ls_tmp_dir1)
            self.assertIn('HITS.10075481._000433.pool.root.1', ls_tmp_dir2)
