#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017


import json
import os

config = None


class Config(object):
    def __init__(self):
        self.items = []

    def __getitem__(self, item):
        return self.items[item]

    def load(self, filename):
        if not os.path.isfile(filename):
            filename = 'pilot_conf.json'

        if not os.path.isfile(filename):
            filename = '/etc/pilot/conf.json'

        if os.path.isfile(filename):
            with open(filename) as f:
                self.items = json.load(f)


if config is None:
    config = Config()
