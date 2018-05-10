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
    """
    Converts the file-with-destination dict to a destination-with-files dict

    :param files Files to merge

    :returns destination-with-files dictionary
    """
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


def get_copysetup(copytools, copytool_name):
    """
    Return the copysetup for the given copytool.

    :param copytools: copytools list from infosys.
    :param copytool name: name of copytool (string).
    :return: copysetup (string).
    """
    copysetup = ""

    for ct in copytools:
        if copytool_name == ct.get('copytool'):
               copysetup = ct.get('copysetup')
            break

    return copysetup
