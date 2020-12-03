# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017

import os
import random
import shutil
import tempfile
import unittest
import uuid

from pilot.api import data


def check_env():
    """
    Function to check whether cvmfs is available.
    To be used to decide whether to skip some test functions.

    :returns True: if unit test should run (currently broken)
    """
    return False


@unittest.skipIf(not check_env(), "This unit test is broken")
class TestHarvesterStageIn(unittest.TestCase):
    """
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
    """

    def setUp(self):
        # skip tests if running through Travis -- github does not have working rucio
        self.travis = os.environ.get('TRAVIS') == 'true'

        # setup pilot data client

        # 1st example: using StageIn client with infosys
        # initialize StageInClient using infosys component to resolve allowed input sources
        #from pilot.info import infosys
        #infosys.init('ANALY_CERN')
        #self.data_client = data.StageInClient(infosys)

        # 2nd example: avoid using infosys instance but it requires to pass explicitly copytools and allowed input Strorages in order to resolve replicas
        #self.data_client = data.StageInClient(acopytools={'pr':'rucio'})
        self.data_client = data.StageInClient(acopytools='rucio')  ## use rucio everywhere

    def test_stagein_sync_fail_nodirectory(self):
        '''
        Test error message propagation.
        '''
        if self.travis:
            return True

        result = self.data_client.transfer(files=[{'scope': 'does_not_matter',
                                                   'name': 'does_not_matter',
                                                   'destination': '/i_do_not_exist'},
                                                  {'scope': 'does_not_matter_too',
                                                   'name': 'does_not_matter_too',
                                                   'destination': '/neither_do_i'}])

        self.assertIsNotNone(result)
        for _file in result:
            self.assertEqual(_file['errno'], 1)
            self.assertEqual(_file['status'], 'failed')
            #self.assertIn(_file['errmsg'], ['Destination directory does not exist: /i_do_not_exist',
            #                              'Destination directory does not exist: /neither_do_i'])

    def test_stagein_sync_fail_noexist(self):
        '''
        Test error message propagation.
        '''
        if self.travis:
            return True

        result = self.data_client.transfer(files=[{'scope': 'no_scope1',
                                                   'name': 'no_name1',
                                                   'destination': '/tmp'},
                                                  {'scope': 'no_scope2',
                                                   'name': 'no_name2',
                                                   'destination': '/tmp'}])

        self.assertIsNotNone(result)
        for _file in result:
            self.assertEqual(_file['errno'], 3)
            self.assertEqual(_file['status'], 'failed')
            #self.assertIn(_file['errmsg'], ['Data identifier \'no_scope1:no_name1\' not found',
            #                               'Data identifier \'no_scope2:no_name2\' not found'])

    def test_stagein_sync_fail_mix(self):
        '''
        Test error message propagation
        '''
        if self.travis:
            return True

        ## if infosys was not passed to StageInClient in constructor
        ## then it's mandatory to specify allowed `inputddms` that can be used as source for replica lookup
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

        self.assertIsNotNone(result)
        for _file in result:
            if _file['name'] in ['no_name1', 'no_name2']:
                self.assertEqual(_file['errno'], 3)
                self.assertEqual(_file['status'], 'failed')
                #self.assertIn(_file['errmsg'], ['Data identifier \'no_scope1:no_name1\' not found',
                #                               'Data identifier \'no_scope2:no_name2\' not found'])
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

        self.assertIsNotNone(result)
        for _file in result:
            self.assertEqual(_file['errno'], 0)

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

        self.assertIsNotNone(result)
        for _file in result:
            self.assertEqual(_file['errno'], 0)

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

        self.assertIsNotNone(result)
        for _file in result:
            self.assertEqual(_file['errno'], 0)
            self.assertIn('HITS.10075481._000432.pool.root.1', ls_tmp_dir1)
            self.assertIn('HITS.10075481._000433.pool.root.1', ls_tmp_dir2)


@unittest.skipIf(not check_env(), "This unit test is broken")
class TestHarvesterStageOut(unittest.TestCase):
    '''
    Automatic stage-out tests for Harvester.

        from pilot.api import data
        data_client = data.StageOutClient(site)
        result = data_client.transfer(files=[{scope, name, ...}, ...])
    '''

    def setUp(self):
        # skip tests if running through Travis -- github does not have working rucio

        self.travis = os.environ.get('TRAVIS') == 'true'

        # setup pilot data client
        self.data_client = data.StageOutClient(acopytools=['rucio'])

    def test_stageout_fail_notfound(self):
        '''
        Test error message propagation.
        '''
        if self.travis:
            return True

        result = self.data_client.transfer(files=[{'scope': 'tests',
                                                   'file': 'i_do_not_exist',
                                                   'rse': 'CERN-PROD_SCRATCHDISK'},
                                                  {'scope': 'tests',
                                                   'file': 'neither_do_i',
                                                   'rse': 'CERN-PROD_SCRATCHDISK'}])

        for _file in result:
            self.assertEqual(_file['errno'], 1)

    def test_stageout_file(self):
        '''
        Single file upload with various combinations of parameters.
        '''
        if self.travis:
            return True

        tmp_fd, tmp_file1 = tempfile.mkstemp()
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file2 = tempfile.mkstemp()
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file3 = tempfile.mkstemp()
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file4 = tempfile.mkstemp()
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()

        result = self.data_client.transfer(files=[{'scope': 'tests',
                                                   'file': tmp_file1,
                                                   'rse': 'CERN-PROD_SCRATCHDISK'},
                                                  {'scope': 'tests',
                                                   'file': tmp_file2,
                                                   'lifetime': 600,
                                                   'rse': 'CERN-PROD_SCRATCHDISK'},
                                                  {'scope': 'tests',
                                                   'file': tmp_file3,
                                                   'lifetime': 600,
                                                   'summary': True,
                                                   'rse': 'CERN-PROD_SCRATCHDISK'},
                                                  {'scope': 'tests',
                                                   'file': tmp_file4,
                                                   'guid': str(uuid.uuid4()),
                                                   'rse': 'CERN-PROD_SCRATCHDISK'}])

        for _file in result:
            self.assertEqual(_file['errno'], 0)

    def test_stageout_file_and_attach(self):
        '''
        Single file upload and attach to dataset.
        '''
        if self.travis:
            return True

        tmp_fd, tmp_file1 = tempfile.mkstemp()
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file2 = tempfile.mkstemp()
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()

        result = self.data_client.transfer(files=[{'scope': 'tests',
                                                   'file': tmp_file1,
                                                   'lifetime': 600,
                                                   'rse': 'CERN-PROD_SCRATCHDISK',
                                                   'attach': {'scope': 'tests',
                                                              'name': 'pilot2.tests.test_harvester'}},
                                                  {'scope': 'tests',
                                                   'file': tmp_file2,
                                                   'rse': 'CERN-PROD_SCRATCHDISK',
                                                   'attach': {'scope': 'tests',
                                                              'name': 'pilot2.tests.test_harvester'}}])

        for _file in result:
            self.assertEqual(_file['errno'], 0)

    def test_stageout_file_noregister(self):
        '''
        Single file upload without registering.
        '''
        if self.travis:
            return True

        tmp_fd, tmp_file1 = tempfile.mkstemp()
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file2 = tempfile.mkstemp()
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()

        result = self.data_client.transfer(files=[{'scope': 'tests',
                                                   'file': tmp_file1,
                                                   'rse': 'CERN-PROD_SCRATCHDISK',
                                                   'no_register': True},
                                                  {'scope': 'tests',
                                                   'file': tmp_file2,
                                                   'rse': 'CERN-PROD_SCRATCHDISK',
                                                   'no_register': True}])

        for _file in result:
            self.assertEqual(_file['errno'], 0)

    def test_stageout_dir(self):
        '''
        Single file upload.
        '''
        if self.travis:
            return True

        tmp_dir = tempfile.mkdtemp()
        tmp_fd, tmp_file1 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file2 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file3 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()

        result = self.data_client.transfer(files=[{'scope': 'tests',
                                                   'file': tmp_dir,
                                                   'rse': 'CERN-PROD_SCRATCHDISK'}])

        for _file in result:
            self.assertEqual(_file['errno'], 0)

    def test_stageout_dir_and_attach(self):
        '''
        Single file upload and attach to dataset.
        '''
        if self.travis:
            return True

        tmp_dir = tempfile.mkdtemp()
        tmp_fd, tmp_file1 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file2 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file3 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()

        result = self.data_client.transfer(files=[{'scope': 'tests',
                                                   'file': tmp_dir,
                                                   'lifetime': 600,
                                                   'rse': 'CERN-PROD_SCRATCHDISK',
                                                   'attach': {'scope': 'tests',
                                                              'name': 'pilot2.tests.test_harvester'}}])

        for _file in result:
            self.assertEqual(_file['errno'], 0)

    def test_stageout_dir_noregister(self):
        '''
        Single file upload without registering.
        '''
        if self.travis:
            return True

        tmp_dir = tempfile.mkdtemp()
        tmp_fd, tmp_file1 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file2 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()
        tmp_fd, tmp_file3 = tempfile.mkstemp(dir=tmp_dir)
        tmp_fdo = os.fdopen(tmp_fd, 'wb')
        tmp_fdo.write(str(random.randint(1, 2**2048)))
        tmp_fdo.close()

        result = self.data_client.transfer(files=[{'scope': 'tests',
                                                   'file': tmp_dir,
                                                   'no_register': True,
                                                   'rse': 'CERN-PROD_SCRATCHDISK'}])

        for _file in result:
            self.assertEqual(_file['errno'], 0)
