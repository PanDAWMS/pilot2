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
import shutil
import random
import os
import os.path

from pilot.copytool.mv import copy_in, copy_out
from pilot.common.exception import StageInFailure, StageOutFailure
from pilot.util.container import execute


class TestCopytoolMv(unittest.TestCase):
    """
    Unit tests for mv copytool.
    """

    filelist = []
    numFiles = 10
    maxFileSize = 100 * 1024

    def setUp(self):
        """ Create temp destination directory """
        self.tmp_dst_dir = tempfile.mkdtemp()

        """ Create temp source directory """
        self.tmp_src_dir = tempfile.mkdtemp()

        self.filelist = []
        """ Create temp files in source dir """
        for i in range(0, self.numFiles):
                # generate random name
                fname = ''.join(random.choice(string.lowercase) for x in range(20))
                # generate random data and write
                fsize = random.randint(1, self.maxFileSize)
                data = [random.randint(0, 255) for x in range(0, fsize)]
                new_file = open(os.path.join(self.tmp_src_dir, fname), "wb")
                new_file.write(str(data))
                new_file.close()
                # add to list
                self.filelist.append({'name': fname, 'source': self.tmp_src_dir, 'destination': self.tmp_dst_dir})

    def test_copy_in_mv(self):
        _, stdout1, stderr1 = execute(' '.join(['ls', self.tmp_src_dir]))
        copy_in(self.filelist)
        # here check files copied
        self.assertEqual(self.__dirs_content_valid(self.tmp_src_dir, self.tmp_dst_dir, dir1_expected_content='', dir2_expected_content=stdout1), 0)

    def test_copy_in_cp(self):
        copy_in(self.filelist, copy_type='cp')
        self.assertEqual(self.__dirs_content_equal(self.tmp_src_dir, self.tmp_dst_dir), 0)

    def test_copy_in_symlink(self):
        copy_in(self.filelist, copy_type='symlink')
        # here check files linked
        self.assertEqual(self.__dirs_content_equal(self.tmp_src_dir, self.tmp_dst_dir), 0)
        # check dst files are links
        _, stdout, _ = execute('find %s -type l -exec echo -n l \;' % self.tmp_dst_dir)
        self.assertEqual(stdout, ''.join('l' for i in range(self.numFiles)))

    def test_copy_in_invalid(self):
        self.assertRaises(StageInFailure, copy_in, self.filelist, **{'copy_type': ''})
        self.assertRaises(StageInFailure, copy_in, self.filelist, **{'copy_type': None})

    def test_copy_out_mv(self):
        _, stdout1, stderr1 = execute(' '.join(['ls', self.tmp_src_dir]))
        copy_out(self.filelist)
        # here check files linked
        self.assertEqual(self.__dirs_content_valid(self.tmp_src_dir, self.tmp_dst_dir, dir1_expected_content='', dir2_expected_content=stdout1), 0)

    def test_copy_out_cp(self):
        copy_out(self.filelist, copy_type='cp')
        self.assertEqual(self.__dirs_content_equal(self.tmp_src_dir, self.tmp_dst_dir), 0)

    def test_copy_out_invalid(self):
        self.assertRaises(StageOutFailure, copy_out, self.filelist, **{'copy_type': ''})
        self.assertRaises(StageOutFailure, copy_out, self.filelist, **{'copy_type': 'symlink'})
        self.assertRaises(StageOutFailure, copy_out, self.filelist, **{'copy_type': None})

    def tearDown(self):
        """ Drop temp directories """
        shutil.rmtree(self.tmp_dst_dir)
        shutil.rmtree(self.tmp_src_dir)

    def __dirs_content_equal(self, dir1, dir2):
        if dir1 == '' or dir2 == '' or dir1 is None or dir2 is None:
            return -1
        _, stdout1, stderr1 = execute(' '.join(['ls', dir1]))
        _, stdout2, stderr2 = execute(' '.join(['ls', dir2]))
        if stdout1 != stdout2:
            return -2
        return 0

    def __dirs_content_valid(self, dir1, dir2, dir1_expected_content=None, dir2_expected_content=None):
        if dir1 == '' or dir2 == '' or dir1 is None or dir2 is None:
            return -1
        _, stdout1, stderr1 = execute(' '.join(['ls', dir1]))
        if dir1_expected_content is not None and stdout1 != dir1_expected_content:
            return -3
        _, stdout2, stderr2 = execute(' '.join(['ls', dir2]))
        if dir2_expected_content is not None and stdout2 != dir2_expected_content:
            return -4
        return 0


if __name__ == '__main__':
    unittest.main()
