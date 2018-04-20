#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018

import os

from pilot.copytool.common import merge_destinations
from pilot.util.container import execute

def is_valid_for_copy_in(files):
    for f in files:
        if not all(key in f for key in ('scope', 'name', 'destination'))
            return False
    return True

def is_valid_for_copy_out(files):
    for f in files:
        if not all(key in f for key in ('file', 'rse'))
            return False
    return True

def copy_in(files):
    """
    Tries to download the given files using rucio.

    :param files: Files to download

    :raises Exception
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    destinations = merge_destinations(files)

    for dst in destinations:
        executable = ['/usr/bin/env',
                      'rucio', 'download',
                      '--no-subdir',
                      '--dir', dst]
        executable.extend(destinations[dst]['lfns'])

        exit_code, stdout, stderr = execute(" ".join(executable))

        stats = {}
        if exit_code == 0:
            stats['status'] = 'done'
            stats['errno'] = 0
            stats['errmsg'] = 'File successfully downloaded.'
        else:
            stats['status'] = 'failed'
            stats['errno'] = 3
            try:
                # the Details: string is set in rucio: lib/rucio/common/exception.py in __str__()
                stats['errmsg'] = [detail for detail in stderr.split('\n') if detail.startswith('Details:')][0][9:-1]
            except Exception as e:
                stats['errmsg'] = 'Could not find rucio error message details - please check stderr directly: %s' % \
                                  str(e)
        for f in destinations[dst]['files']:
            f.update(stats)
    return files


def copy_out(files):
    """
    Tries to upload the given files using rucio

    :param files Files to download. Dictionary with:
        file:           - file path of the file to upload
        rse:            - storage endpoint
        scope:          - Optional: scope of the file
        no-register:    - Optional: Do not register the file in rucio
        guid:           - Optional: guid to use for the file
        pfn:            - Optional: pfn to use for the upload
        lifetime:       - Optional: lifetime on storage for this file

    :raises Exception
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    if len(files) == 0:
        raise Exception('No lfn with existing destination path given!')

    for f in files:
        executable = ['/usr/bin/env', 'rucio', 'upload']
        path = f.get('file')
        rse = f.get('rse')
        exit_code = 1
        if path and rse:
            executable.extend(['--rse', rse])

            scope = f.get('scope')
            guid = f.get('guid')
            pfn = f.get('pfn')
            lifetime = f.get('lifetime')
            
            if scope:
                executable.extend(['--scope', scope])
            if guid:
                executable.extend(['--guid', guid])
            if pfn:
                executable.extend(['--pfn', pfn])
            if lifetime:
                executable.extend(['--lifetime', lifetime])
            if 'no-register' in f:
                executable.extend(['--no-register'])

            executable.append(path)

            exit_code, stdout, stderr = execute(" ".join(executable))

        stats = {}
        if exit_code == 0:
            stats['status'] = 'done'
            stats['errno'] = 0
            stats['errmsg'] = 'File successfully downloaded.'
        else:
            stats['status'] = 'failed'
            stats['errno'] = 3
            try:
                # the Details: string is set in rucio: lib/rucio/common/exception.py in __str__()
                stats['errmsg'] = [detail for detail in stderr.split('\n') if detail.startswith('Details:')][0][9:-1]
            except Exception as e:
                stats['errmsg'] = 'Could not find rucio error message details - please check stderr directly: %s' % \
                                  str(e)
    return files
