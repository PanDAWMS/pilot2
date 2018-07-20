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
