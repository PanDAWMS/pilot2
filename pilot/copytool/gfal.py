#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2017

import os
import logging
import shutil
import errno

from pilot.common.exception import StageInFailure, StageOutFailure
from pilot.util.container import execute

logger = logging.getLogger(__name__)


def copy_in(files):
    """
    Tries to download the given files using mv directly.

    :param files: Files to download
    :raises PilotException: StageInFailure
    """

    if __check_for_gfal():
        raise StageInFailure("No GFAL2 tools found")
    exit_code, stdout, stderr = move_all_files(files, copy_type)
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)


def copy_out(files):
    """
    Tries to upload the given files using mv directly.

    :param files: Files to upload
    :raises PilotException: StageOutFailure
    """

    if __check_for_gfal():
        raise StageOutFailure("No GFAL2 tools found")

    exit_code, stdout, stderr = move_all_files(files, copy_type)
    if exit_code != 0:
        # raise failure
        raise StageOutFailure(stdout)


def move_all_files(files, copy_type, nretries=1):
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

        source = os.path.join(entry['source'], entry['name'])
        destination = os.path.join(entry['destination'], entry['name'])
        retry = nretries
        while retry != 0:
            retry -= 1
            exit_code, stdout, stderr = __move(source, destination)
            if exit_code != 0:
                if ((exit_code != errno.ETIMEDOUT) and (exit_code != errno.ETIME)) or retry == 0:
                    logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))
                    return exit_code, stdout, stderr
            else: # all successful
                break

    return exit_code, stdout, stderr


def __move(source, destination):
    cmd = "gfal-copy %s %s" % (source, destination)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr


def __check_for_gfal():
    gfal_path = shutil.which('gfal-copy')
    return gfal_path is not None
