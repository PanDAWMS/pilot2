#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2018

import os
import logging
import errno
import re

from pilot.common.exception import StageInFailure, StageOutFailure, PilotException, ErrorCodes
from pilot.util.container import execute
from pilot.copytool.common import get_copysetup

logger = logging.getLogger(__name__)

require_replicas = True  ## indicate if given copytool requires input replicas to be resolved

allowed_schemas = ['srm', 'gsiftp', 'root']  # prioritized list of supported schemas for transfers by given copytool


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER
    for f in files:
        if not all(key in f for key in ('name', 'source', 'destination')):
            return False
    return True


def is_valid_for_copy_out(files):
    for f in files:
        if not all(key in f for key in ('name', 'source', 'destination')):
            return False
    return True


def copy_in_old(files):
    """
    Tries to download the given files using lsm-get directly.

    :param files: Files to download
    :raises PilotException: StageInFailure
    """

    if not check_for_lsm(dst_in=True):
        raise StageInFailure("No LSM tools found")
    exit_code, stdout, stderr = move_all_files_in(files)
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)


def get_timeout(filesize):   ## ISOLATE ME LATER
    """ Get a proper time-out limit based on the file size """

    timeout_max = 3 * 3600  # 3 hours
    timeout_min = 300  # self.timeout

    timeout = timeout_min + int(filesize / 0.5e6)  # approx < 0.5 Mb/sec

    return min(timeout, timeout_max)


def copy_in(files, **kwargs):
    """
        Download given files using the LSM command.

        :param files: list of `FileSpec` objects
        :raise: PilotException in case of controlled error
    """

    exit_code = 0
    stdout = ""
    stderr = ""

    #nretries = kwargs.get('nretries') or 1
    copytools = kwargs.get('copytools') or []
    copysetup = get_copysetup(copytools, 'lsm')

    for fspec in files:

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        timeout = get_timeout(fspec.filesize)
        source = fspec.turl
        destination = os.path.join(dst, fspec.lfn)

        logger.info("transferring file %s from %s to %s" % (fspec.lfn, source, destination))

        exit_code, stdout, stderr = move(source, destination, dst_in=True, copysetup=copysetup)

        if exit_code != 0:
            logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))

            error = resolve_transfer_error(stderr, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('exit_code')
            raise PilotException(error.get('error'), code=error.get('exit_code'), state=error.get('state'))

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
    Tries to upload the given files using lsm-put directly.

    :param files: Files to upload
    :raises PilotException: StageOutFailure
    """

    if not check_for_lsm(dst_in=False):
        raise StageOutFailure("No LSM tools found")

    exit_code, stdout, stderr = move_all_files_out(files)
    if exit_code != 0:
        # raise failure
        raise StageOutFailure(stdout)


def move_all_files_in(files, nretries=1):
    """
    Move all files.

    :param files:
    :param nretries: number of retries; sometimes there can be a timeout copying, but the next attempt may succeed
    :return: exit_code, stdout, stderr
    """

    exit_code = 0
    stdout = ""
    stderr = ""

    for entry in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}
        logger.info("transferring file %s from %s to %s" % (entry['name'], entry['source'], entry['destination']))

        source = entry['source'] + '/' + entry['name']
        destination = os.path.join(entry['destination'], entry['name'])
        for retry in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=True)

            if exit_code != 0:
                if ((exit_code != errno.ETIMEDOUT) and (exit_code != errno.ETIME)) or (retry + 1) == nretries:
                    logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))
                    return exit_code, stdout, stderr
            else:  # all successful
                break

    return exit_code, stdout, stderr


def move_all_files_out(files, nretries=1):
    """
    Move all files.

    :param files:
    :return: exit_code, stdout, stderr
    """

    exit_code = 0
    stdout = ""
    stderr = ""

    for entry in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}
        logger.info("transferring file %s from %s to %s" % (entry['name'], entry['source'], entry['destination']))

        destination = entry['destination'] + '/' + entry['name']
        source = os.path.join(entry['source'], entry['name'])
        for retry in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=False)

            if exit_code != 0:
                if ((exit_code != errno.ETIMEDOUT) and (exit_code != errno.ETIME)) or (retry + 1) == nretries:
                    logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))
                    return exit_code, stdout, stderr
            else:  # all successful
                break

    return exit_code, stdout, stderr


def move(source, destination, dst_in=True, copysetup=""):
    """
    Use lsm-get or lsm-put to transfer the file.
    :param source: path to source (string).
    :param destination: path to destination (string).
    :param dst_in: True for stage-in, False for stage-out (boolean).
    :return: exit code, stdout, stderr
    """

    if copysetup != "":
        cmd = 'source %s;' % copysetup
    else:
        cmd = ''
    if dst_in:
        cmd += "which lsm-get;lsm-get %s %s" % (source, destination)
    else:
        cmd += "lsm-put %s %s" % (source, destination)
    logger.info("Using copy command: %s" % cmd)
    exit_code, stdout, stderr = execute(cmd)
    logger.info('stdout=%s'%stdout)
    return exit_code, stdout, stderr


def check_for_lsm(dst_in=True):
    cmd = None
    if dst_in:
        cmd = 'which lsm-get'
    else:
        cmd = 'which lsm-put'
    exit_code, gfal_path, _ = execute(cmd)
    return exit_code == 0
