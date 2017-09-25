#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os


def merge_destinations(files):
    destinations = {}
    # ensure type(files) == list
    for f in files:
        # ensure destination in f
        if not os.path.exists(f['destination']):
            f['status'] = 'failed'
            f['errmsg'] = 'Destination directory does not exist: %s' % f['destination']
            f['errno'] = 1
        else:
            # ensure scope, name in f
            f['status'] = 'running'
            f['errmsg'] = 'File not yet successfully downloaded.'
            f['errno'] = 2
            lfn = '%s:%s' % (f['scope'], f['name'])
            dst = destinations.setdefault(f['destination'], {'lfns': set(), 'files': list()})
            dst['lfns'].add(lfn)
            dst['files'].append(f)
    return destinations
