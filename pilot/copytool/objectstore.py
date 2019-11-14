#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2019
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

import os
import json
import logging

from .common import resolve_common_transfer_errors
from pilot.common.exception import PilotException, ErrorCodes
#from pilot.info.storageactivitymaps import get_ddm_activity
from pilot.util.container import execute
from pilot.util.ruciopath import get_rucio_path

logger = logging.getLogger(__name__)

# can be disable for Rucio if allowed to use all RSE for input
require_replicas = False    ## indicates if given copytool requires input replicas to be resolved

require_input_protocols = True    ## indicates if given copytool requires input protocols and manual generation of input replicas
require_protocols = True  ## indicates if given copytool requires protocols to be resolved first for stage-out

allowed_schemas = ['srm', 'gsiftp', 'https', 'davs', 'root', 's3', 's3+rucio']


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files):
    return True  ## FIX ME LATER


def resolve_surl(fspec, protocol, ddmconf, **kwargs):
    """
        Get final destination SURL for file to be transferred to Objectstore
        Can be customized at the level of specific copytool

        :param protocol: suggested protocol
        :param ddmconf: full ddm storage data
        :param fspec: file spec data
        :return: dictionary {'surl': surl}
    """
    ddm = ddmconf.get(fspec.ddmendpoint)
    if not ddm:
        raise PilotException('Failed to resolve ddmendpoint by name=%s' % fspec.ddmendpoint)

    if ddm.is_deterministic:
        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), get_rucio_path(fspec.scope, fspec.lfn))
    elif ddm.type in ['OS_ES', 'OS_LOGS']:
        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), fspec.lfn)
        fspec.protocol_id = protocol.get('id')
    else:
        raise PilotException('resolve_surl(): Failed to construct SURL for non deterministic ddm=%s: NOT IMPLEMENTED', fspec.ddmendpoint)

    return {'surl': surl}

## redundant logic, can be removed (anisyonk)
#def resolve_protocol(fspec, activity, ddm):
#    """
#        Rosolve protocols to be used to transfer the file with corressponding activity
#
#        :param fspec: file spec data
#        :param activity: actvitiy name as string
#        :param ddm: ddm storage data
#        :return: protocol as dictionary
#    """
#
#    logger.info("Resolving protocol for file(lfn: %s, ddmendpoint: %s) with activity(%s)" % (fspec.lfn, fspec.ddmendpoint, activity))
#
#    activity = get_ddm_activity(activity)
#    protocols = ddm.arprotocols.get(activity)
#    protocols_allow = []
#    for schema in allowed_schemas:
#        for protocol in protocols:
#            if schema is None or protocol.get('endpoint', '').startswith("%s://" % schema):
#                protocols_allow.append(protocol)
#    if not protocols_allow:
#        err = "No available allowed protocols for file(lfn: %s, ddmendpoint: %s) with activity(%s)" % (fspec.lfn, fspec.ddmendpoint, activity)
#        logger.error(err)
#        raise PilotException(err)
#    protocol = protocols_allow[0]
#    logger.info("Resolved protocol for file(lfn: %s, ddmendpoint: %s) with activity(%s): %s" % (fspec.lfn, fspec.ddmendpoint, activity, protocol))
#    return protocol


def copy_in(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :raise: PilotException in case of controlled error
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    ddmconf = kwargs.pop('ddmconf', {})
    #activity = kwargs.pop('activity', None)
    # trace_report = kwargs.get('trace_report')

    for fspec in files:

        cmd = []
        logger.info("To transfer file: %s" % fspec)
        if fspec.protocol_id:
            ddm = ddmconf.get(fspec.ddmendpoint)
            if ddm:
                ddm_special_setup = ddm.get_special_setup(fspec.protocol_id)
                if ddm_special_setup:
                    cmd = [ddm_special_setup]

        # redundant logic: to be cleaned (anisyonk)
        #ddm = ddmconf.get(fspec.ddmendpoint)
        #if ddm:
        #    protocol = resolve_protocol(fspec, activity, ddm)
        #    surls = resolve_surl(fspec, protocol, ddmconf)
        #    if 'surl' in surls:
        #        fspec.surl = surls['surl']
        #    ddm_special_setup = ddm.get_special_setup(fspec.protocol_id)
        #    if ddm_special_setup:
        #        cmd += [ddm_special_setup]

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        cmd += ['/usr/bin/env', 'rucio', '-v', 'download', '--no-subdir', '--dir', dst]
        if require_replicas:
            cmd += ['--rse', fspec.replicas[0]['ddmendpoint']]

        # a copytool module should consider fspec.turl for transfers, and could failback to fspec.surl,
        # but normally fspec.turl (transfer url) is mandatory and already populated by the top workflow
        turl = fspec.turl or fspec.surl
        if turl:
            if fspec.ddmendpoint:
                cmd.extend(['--rse', fspec.ddmendpoint])
            cmd.extend(['--pfn', turl])
        cmd += ['%s:%s' % (fspec.scope, fspec.lfn)]

        rcode, stdout, stderr = execute(" ".join(cmd), **kwargs)

        if rcode:  ## error occurred
            error = resolve_common_transfer_errors(stderr, is_stagein=True)
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
    ddmconf = kwargs.pop('ddmconf', {})
    # trace_report = kwargs.get('trace_report')

    for fspec in files:
        cmd = []
        if fspec.protocol_id:
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
                        raise PilotException("Failed to stageout: CRC mismatched", code=ErrorCodes.PUTADMISMATCH, state='AD_MISMATCH')

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files
