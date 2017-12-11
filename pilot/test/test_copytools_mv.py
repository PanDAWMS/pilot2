#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Pavlo Svirin, pavlo.svirin@cern.ch

import unittest
import string
import tempfile
import shutil
import random
import os.path
import sys

#from pilot.util.workernode import collect_workernode_info, get_disk_space_for_dispatcher
from pilot.copytool.mv import *
from pilot.common.exception import StageInFailure, StageOutFailure


class TestCopytoolMv(unittest.TestCase):
    """
    Unit tests for mv copytool.
    """
    
    filelist = []
    numFiles = 10
    maxFileSize = 100*1024

    def setUp(self):
        """ Create temp destination directory """
        self.tmp_dst_dir = tempfile.mkdtemp()

        """ Create temp source directory """
        self.tmp_src_dir = tempfile.mkdtemp()

        print("Temporary source directory: %s" % self.tmp_src_dir)
        print("Temporary destination directory: %s" % self.tmp_dst_dir)

        self.filelist = []
        """ Create temp files in source dir """
        for i in range (0, self.numFiles):
                # generate random name
                fname = ''.join(random.choice(string.lowercase) for x in range(20))
                # generate random data and write
                fsize = random.randint(1, self.maxFileSize)
                data = [ random.randint(0, 255) for x in range(0,fsize) ]
                newFile = open(os.path.join(self.tmp_src_dir, ''.join(random.choice(string.lowercase) for x in range(20))), "wb")
                newFile.write(str(data))
                newFile.close()
                # add to list
                self.filelist.append({'name': fname, 'source': self.tmp_src_dir, 'destination': self.tmp_dst_dir})
        #print(self.filelist)

    def test_copy_in_mv(self):
        copy_in(self.filelist)
        #self.assertEqual(exit_code, 0)
        # here check files copied

    def test_copy_in_cp(self):
        print "Copy in:"
        print(self.filelist)
        copy_in(self.filelist, copy_type='cp')
        #self.assertEqual(exit_code, 0)
        # here check files copied

    def test_copy_in_symlink(self):
        copy_in(self.filelist, copy_type='symlink')
        # here check files linked

    def test_copy_in_invalid(self):
        self.assertRaises(StageInFailure,  copy_in, self.filelist, **{'copy_type' : ''})
        self.assertRaises(StageInFailure,  copy_in, self.filelist, **{'copy_type' : None})
        #self.assertRaises(StageInFailure, copy_in, self.filelist, copy_type=None)

    @unittest.skip("demonstrating skipping")
    def test_copy_out_mv(self):
        pass

    @unittest.skip("demonstrating skipping")
    def test_copy_out_cp(self):
        pass

    @unittest.skip("demonstrating skipping")
    def test_copy_out_invalid(self):
        self.assertRaises(StageOutFailure,  copy_in, self.filelist, **{'copy_type' : ''})
        self.assertRaises(StageOutFailure,  copy_in, self.filelist, **{'copy_type' : 'symlink'})
        self.assertRaises(StageOutFailure,  copy_in, self.filelist, **{'copy_type' : None})
        #copy_out(self.filelist, copy_type='symlink')
        #copy_out(self.filelist, copy_type=None)
        # here check files linked
        pass

    def tearDown(self):
        """ Drop temp directories """
        shutil.rmtree(self.tmp_dst_dir)
        print("Dropping: " + self.tmp_dst_dir)
        shutil.rmtree(self.tmp_src_dir)
        print("Dropping: " + self.tmp_src_dir)

if __name__ == '__main__':
    unittest.main()


'''
    def test_collect_workernode_info(self):
        """
        Make sure that collect_workernode_info() returns the proper types (float, float).

        :return: (assertion)
        """

        mem, cpu = collect_workernode_info()

        self.assertEqual(type(mem), float)
        self.assertEqual(type(cpu), float)

        self.assertNotEqual(mem, 0.0)
        self.assertNotEqual(cpu, 0.0)

    def test_get_disk_space_for_dispatcher(self):
        """
        Verify that get_disk_space_for_dispatcher() returns the proper type (int).

        :return: (assertion)
        """

        queuedata = {'maxwdir': 123456789}
        diskspace = get_disk_space_for_dispatcher(queuedata)

        self.assertEqual(type(diskspace), int)
'''
