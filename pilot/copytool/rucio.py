#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import os
import json
import logging

from .common import resolve_common_transfer_errors, verify_catalog_checksum
from pilot.common.exception import PilotException, ErrorCodes
from pilot.util.container import execute

logger = logging.getLogger(__name__)

# can be disable for Rucio if allowed to use all RSE for input
require_replicas = True    ## indicates if given copytool requires input replicas to be resolved
require_protocols = False  ## indicates if given copytool requires protocols to be resolved first for stage-out


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files):
    return True  ## FIX ME LATER


def copy_in(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :raise: PilotException in case of controlled error
    """

    allow_direct_access = kwargs.get('allow_direct_access') or False

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    for fspec in files:
        # continue loop for files that are to be accessed directly
        if fspec.is_directaccess(ensure_replica=False) and allow_direct_access:
            fspec.status_code = 0
            fspec.status = 'remote_io'
            continue

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        cmd = ['/usr/bin/env', 'rucio', '-v', 'download', '--no-subdir', '--dir', dst, '--pfn', fspec.turl]
        if require_replicas:
            cmd += ['--rse', fspec.replicas[0][0]]
        cmd += ['%s:%s' % (fspec.scope, fspec.lfn)]

        rcode, stdout, stderr = execute(" ".join(cmd), **kwargs)
        logger.info('stdout = %s' % stdout)
        logger.info('stderr = %s' % stderr)

        if rcode:  ## error occurred
            error = resolve_common_transfer_errors(stderr, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        destination = os.path.join(dst, fspec.lfn)
        if os.path.exists(destination):
            state, diagnostics = verify_catalog_checksum(fspec, destination)
            if diagnostics != "":
                raise PilotException(diagnostics, code=fspec.status_code, state=state)
        else:
            logger.warning('wrong path: %s' % destination)

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def copy_out(files, **kwargs):
    """
        Upload given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :raise: PilotException in case of controlled error
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    no_register = kwargs.pop('no_register', False)
    summary = kwargs.pop('summary', True)

    for fspec in files:
        cmd = ['/usr/bin/env', 'rucio', '-v', 'upload']
        cmd += ['--rse', fspec.ddmendpoint]

        if fspec.scope:
            cmd.extend(['--scope', fspec.scope])
        if fspec.guid:
            cmd.extend(['--guid', fspec.guid])

        if no_register:
            cmd.append('--no-register')

        if summary:
            cmd.append('--summary')

        if fspec.turl:
            cmd.extend(['--pfn', fspec.turl])

        cmd += [fspec.surl]

        rcode, stdout, stderr = execute(" ".join(cmd), **kwargs)
        logger.info('stdout = %s' % stdout)
        logger.info('stderr = %s' % stderr)
        if rcode:  ## error occurred
            error = resolve_common_transfer_errors(stderr, is_stagein=False)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        if summary:  # resolve final pfn (turl) from the summary JSON
            cwd = fspec.workdir or kwargs.get('workdir') or '.'
            path = os.path.join(cwd, 'rucio_upload.json')
            if not os.path.exists(path):
                logger.error('Failed to resolve Rucio summary JSON, wrong path? file=%s' % path)
            else:
                with open(path, 'rb') as f:
                    summary = json.load(f)
                    dat = summary.get("%s:%s" % (fspec.scope, fspec.lfn)) or {}
                    fspec.turl = dat.get('pfn')
                    # quick transfer verification:
                    # the logic should be unified and moved to base layer shared for all the movers
                    adler32 = dat.get('adler32')
                    if fspec.checksum.get('adler32') and adler32 and fspec.checksum.get('adler32') != adler32:
                        logger.warning('checksum verification failed: local %s != remote %s' %
                                       (fspec.checksum.get('adler32'), adler32))
                        raise PilotException("Failed to stageout: CRC mismatched",
                                             code=ErrorCodes.PUTADMISMATCH, state='AD_MISMATCH')

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def copy_out_old(files):   ### NOT USED - TO BE DEPRECATED
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
