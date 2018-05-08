#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018

import os
import re

from pilot.common.exception import PilotException, ErrorCodes

from pilot.util.container import execute

# can be disable for Rucio if allowed to use all RSE for input
require_replicas = True  ## indicate if given copytool requires input replicas to be resolved


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER
    for f in files:
        if not all(key in f for key in ('scope', 'name', 'destination')):
            return False
    return True


def is_valid_for_copy_out(files):
    return True  ## FIX ME LATER
    for f in files:
        if not all(key in f for key in ('file', 'rse')):
            return False
    return True


def copy_in(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :raise: PilotException in case of controlled error
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    for fspec in files:
        dst = fspec.workdir or kwargs.get('workdir') or '.'
        cmd = ['/usr/bin/env', 'rucio', '-v', 'download', '--no-subdir', '--dir', dst]
        if require_replicas:
            cmd += ['--rse', fspec.replicas[0][0]]
        cmd += ['%s:%s' % (fspec.scope, fspec.lfn)]

        rcode, stdout, stderr = execute(" ".join(cmd), **kwargs)

        if rcode:  ## error occurred
            error = resolve_transfer_error(stderr, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def resolve_transfer_error(output, is_stagein):
    """
        Resolve error code, client state and defined error mesage from the output of transfer command
        :return: dict {'rcode', 'state, 'error'}
    """

    ret = {'rcode': ErrorCodes.STAGEINFAILED if is_stagein else ErrorCodes.STAGEOUTFAILED,
           'state': 'COPY_ERROR', 'error': 'Copy operation failed [is_stagein=%s]: %s' % (is_stagein, output)}

    for line in output.split('\n'):
        m = re.search("Details\s*:\s*(?P<error>.*)", line)
        if m:
            ret['error'] = m.group('error')

    return ret


def copy_out(files):
    """
    Tries to upload the given files using rucio

    :param files Files to download. Dictionary with:
        file:           - file path of the file to upload
        rse:            - storage endpoint
        scope:          - Optional: scope of the file
        guid:           - Optional: guid to use for the file
        pfn:            - Optional: pfn to use for the upload
        lifetime:       - Optional: lifetime on storage for this file
        no_register:    - Optional: if True, do not register the file in rucio
        summary:        - Optional: if True, generates a summary json file

    :raises Exception
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    if len(files) == 0:
        raise Exception('No existing source given!')

    for f in files:
        executable = ['/usr/bin/env', 'rucio', 'upload']
        path = f.get('file')
        rse = f.get('rse')

        stats = {'status': 'failed'}
        if not path or not (os.path.isfile(path) or os.path.isdir(path)):
            stats['errmgs'] = 'Source file does not exists'
            stats['errno'] = 1
            f.update(stats)
            continue
        if not rse:
            stats['errmgs'] = 'No destination site given'
            stats['errno'] = 1
            f.update(stats)
            continue

        executable.extend(['--rse', str(rse)])

        scope = f.get('scope')
        guid = f.get('guid')
        pfn = f.get('pfn')
        lifetime = f.get('lifetime')
        no_register = f.get('no_register', False)
        summary = f.get('summary', False)

        if scope:
            executable.extend(['--scope', str(scope)])
        if guid:
            executable.extend(['--guid', str(guid)])
        if pfn:
            executable.extend(['--pfn', pfn])
        if lifetime:
            executable.extend(['--lifetime', str(lifetime)])
        if no_register:
            executable.append('--no-register')
        if summary:
            executable.append('--summary')

        executable.append(path)

        exit_code, stdout, stderr = execute(executable)

        if exit_code == 0:
            stats['status'] = 'done'
            stats['errno'] = 0
            stats['errmsg'] = 'File successfully uploaded.'
        else:
            stats['errno'] = 3
            try:
                # the Details: string is set in rucio: lib/rucio/common/exception.py in __str__()
                stats['errmsg'] = [detail for detail in stderr.split('\n') if detail.startswith('Details:')][0][9:-1]
            except Exception as e:
                stats['errmsg'] = 'Could not find rucio error message details - please check stderr directly: %s' % \
                                  str(e)
        f.update(stats)
    return files
