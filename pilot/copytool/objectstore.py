#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018

import os
import re
import json
import logging

from pilot.common.exception import PilotException, ErrorCodes
from pilot.util.container import execute

logger = logging.getLogger(__name__)

# can be disable for Rucio if allowed to use all RSE for input
require_replicas = False    ## indicates if given copytool requires input replicas to be resolved
require_protocols = True  ## indicates if given copytool requires protocols to be resolved first for stage-out

allowed_schemas = ['srm', 'gsiftp', 'https', 'davs', 'root', 's3', 's3+rucio']


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


def copy_out(files, **kwargs):
    """
        Upload given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :raise: PilotException in case of controlled error
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    no_register = kwargs.pop('no_register', True)
    summary = kwargs.pop('summary', False)
    ddmconf = kwargs.pop('ddmconf', None)

    for fspec in files:
        cmd = []
        ddm = ddmconf.get(fspec.ddmendpoint)
        if ddm:
            ddm_special_setup = ddm.get_special_setup(fspec.protocol_id)
        if ddm_special_setup:
            cmd = [ddm_special_setup]

        cmd += ['/usr/bin/env', 'rucio', '-v', 'upload']
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

        if rcode:  ## error occurred
            error = resolve_transfer_error(stderr, is_stagein=False)
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
                        raise PilotException("Failed to stageout: CRC mismatched", code=ErrorCodes.PUTADMISMATCH, state='AD_MISMATCH')

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
