#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017


class StageInClient(object):

    def __init__(self, local_site=None):
        super(StageInClient, self).__init__()
        if local_site is None:
            raise Exception('Must set local site.')
        self.local_site = local_site

    def transfer(self, files=None):
        client = StageInClientAsync(local_site=self.local_site)
        client.queue(files)
        client.start()
        while client.is_transferring():
            break
        client.finish()

        return {'errno': 0,
                'status': None}


class StageInClientAsync(object):

    def __init__(self, local_site):
        super(StageInClientAsync, self).__init__()
        if local_site is None:
            raise Exception('Must set local site.')
        self.local_site = local_site

    def queue(self, files):
        pass

    def is_transferring(self):
        finished_files = None
        return finished_files

    def start(self):
        pass

    def finish(self):
        pass
