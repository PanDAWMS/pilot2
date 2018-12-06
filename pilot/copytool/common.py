#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import logging
import os
import re

from pilot.common.exception import ErrorCodes
from pilot.util.filehandling import calculate_checksum, get_checksum_type, get_checksum_value

logger = logging.getLogger(__name__)


def verify_catalog_checksum(fspec, path):
    """
    Verify that the local and catalog checksum values are the same.
    The function will update the fspec object.

    :param fspec: FileSpec object for a given file.
    :param path: path to local file (string).
    :return: state (string), diagnostics (string).
    """

    diagnostics = ""
    state = ""

    checksum_type = get_checksum_type(fspec.checksum)
    checksum_catalog = get_checksum_value(fspec.checksum)
    if checksum_type == 'unknown':
        diagnostics = 'unknown checksum type for checksum(catalog): %s' % fspec.checksum
        logger.warning(diagnostics)
        fspec.status_code = ErrorCodes.UNKNOWNCHECKSUMTYPE
        fspec.status = 'failed'
        state = 'UNKNOWN_CHECKSUM_TYPE'
    else:
        checksum_local = calculate_checksum(path, algorithm=checksum_type)
        logger.info('checksum(catalog)=%s (type: %s)' % (checksum_catalog, checksum_type))
        logger.info('checksum(local)=%s' % checksum_local)
        if checksum_local and checksum_local != '' and checksum_local != checksum_catalog:
            diagnostics = 'checksum verification failed: checksum(catalog)=%s != checsum(local)=%s' % \
                          (checksum_catalog, checksum_local)
            logger.warning(diagnostics)
            fspec.status_code = ErrorCodes.GETADMISMATCH if checksum_type == 'ad32' else ErrorCodes.GETMD5MISMATCH
            fspec.status = 'failed'
            state = 'AD_MISMATCH' if checksum_type == 'ad32' else 'MD_MISMATCH'
        else:
            logger.info('catalog and local checksum values are the same')

    return state, diagnostics


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

    for ct in copytools.keys():
        if copytool_name == ct:
            copysetup = copytools[ct].get('setup')
            break

    return copysetup


def resolve_common_transfer_errors(output, is_stagein=True):
    """
    Resolve any common transfer related errors.

    :param output: stdout from transfer command (string).
    :param is_stagein: optional (boolean).
    :return: dict {'rcode', 'state, 'error'}
    """

    ret = {'rcode': ErrorCodes.STAGEINFAILED if is_stagein else ErrorCodes.STAGEOUTFAILED,
           'state': 'COPY_ERROR', 'error': 'Copy operation failed [is_stagein=%s]: %s' % (is_stagein, output)}

    if "timeout" in output:
        ret['rcode'] = ErrorCodes.STAGEINTIMEOUT if is_stagein else ErrorCodes.STAGEOUTTIMEOUT
        ret['state'] = 'CP_TIMEOUT'
        ret['error'] = 'copy command timed out: %s' % output
    elif "does not match the checksum" in output:
        if 'adler32' in output:
            state = 'AD_MISMATCH'
            rcode = ErrorCodes.GETADMISMATCH if is_stagein else ErrorCodes.PUTADMISMATCH
        else:
            state = 'MD5_MISMATCH'
            rcode = ErrorCodes.GETMD5MISMATCH if is_stagein else ErrorCodes.PUTMD5MISMATCH
        ret['rcode'] = rcode
        ret['state'] = state
    elif "query chksum is not supported" in output or "Unable to checksum" in output:
        ret['rcode'] = ErrorCodes.CHKSUMNOTSUP
        ret['state'] = 'CHKSUM_NOTSUP'
        ret['error'] = output
    elif "Could not establish context" in output:
        ret['rcode'] = ErrorCodes.NOPROXY
        ret['state'] = 'CONTEXT_FAIL'
        ret['error'] = "Could not establish context: Proxy / VO extension of proxy has probably expired: %s" % output
    elif "File exists" in output or 'SRM_FILE_BUSY' in output or 'file already exists' in output:
        ret['rcode'] = ErrorCodes.FILEEXISTS
        ret['state'] = 'FILE_EXISTS'
        ret['error'] = "File already exists in the destination: %s" % output
    elif "No space left on device" in output:
        ret['rcode'] = ErrorCodes.NOLOCALSPACE
        ret['state'] = 'NO_SPACE'
        ret['error'] = "No available space left on local disk: %s" % output
    elif "globus_xio:" in output:
        if is_stagein:
            ret['rcode'] = ErrorCodes.GETGLOBUSSYSERR
        else:
            ret['rcode'] = ErrorCodes.PUTGLOBUSSYSERR
        ret['state'] = 'GLOBUS_FAIL'
        ret['error'] = "Globus system error: %s" % output
    elif "No such file or directory" in output:
        ret['rcode'] = ErrorCodes.NOSUCHFILE
        ret['state'] = 'NO_FILE'
        ret['error'] = output
    elif "service is not available at the moment" in output:
        ret['rcode'] = ErrorCodes.SERVICENOTAVAILABLE
        ret['state'] = 'SERVICE_ERROR'
        ret['error'] = output
    else:
        for line in output.split('\n'):
            m = re.search("Details\s*:\s*(?P<error>.*)", line)
            if m:
                ret['error'] = m.group('error')
            elif 'service_unavailable' in line:
                ret['error'] = 'service_unavailable'
                ret['rcode'] = ErrorCodes.RUCIOSERVICEUNAVAILABLE

    return ret
