# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017

import unittest

from pilot.api.data import StageInClient, StageInClientAsync


class TestHarvester(unittest.TestCase):

    def test_stagein(self):
        client = StageInClient(local_site='CERN-PROD')
        status = client.transfer(files=[{'scope': 'my_scope',
                                         'name': 'my_file1',
                                         'dest_dir': 'this/goes/here'},
                                        {'scope': 'my_scope',
                                         'name': 'my_file2',
                                         'dest_dir': 'and/this/goes/here'}])
        assert status

    def test_stagein_async(self):

        files1 = [{'scope': 'my_scope',
                   'name': 'my_file1',
                   'dest_dir': 'this/goes/here'},
                  {'scope': 'my_scope',
                   'name': 'my_file2',
                   'dest_dir': 'and/this/goes/here'}]

        files2 = [{'scope': 'my_scope',
                   'name': 'my_file3',
                   'dest_dir': 'this/goes/here'},
                  {'scope': 'my_scope',
                   'name': 'my_file4',
                   'dest_dir': 'and/this/goes/here'}]

        client = StageInClientAsync(local_site='CERN-PROD')
        client.start()

        client.queue(files1)
        while client.is_transferring():
            break

        client.queue(files2)
        while client.is_transferring():
            break

        client.finish()
