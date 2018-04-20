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

from pilot.common.exception import StageInFailure, StageOutFailure
from pilot.util.container import execute

logger = logging.getLogger(__name__)

def is_valid_for_copy_in(files):
    for f in files:
        if not all(key in f for key in ('name', 'source', 'destination'))
            return False
    return True

def is_valid_for_copy_out(files):
    for f in files:
        if not all(key in f for key in ('name', 'source', 'destination'))
            return False
    return True

def copy_in(files):
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


def move(source, destination, dst_in=True):
    cmd = None
    if dst_in:
        cmd = "lsm-get %s %s" % (source, destination)
    else:
        cmd = "lsm-put %s %s" % (source, destination)
    logger.info("Using copy command: %s" % cmd)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr


def check_for_lsm(dst_in=True):
    cmd = None
    if dst_in:
        cmd = 'which lsm-get'
    else:
        cmd = 'which lsm-put'
    exit_code, gfal_path, _ = execute(cmd)
    return exit_code == 0
