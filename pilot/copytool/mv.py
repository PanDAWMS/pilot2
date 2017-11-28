#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os

from pilot.common.exception import StageInFailure, StageOutFailure
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def copy_in(files):
    """
    Tries to download the given files using mv directly.

    :param files: Files to download
    :raises PilotException: StageInFailure
    """

    exit_code, stdout, stderr = move_all_files(files)
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)


def copy_out(files):
    """
    Tries to upload the given files using mv directly.

    :param files: Files to upload
    :raises PilotException: StageOutFailure
    """

    exit_code, stdout, stderr = move_all_files(files)
    if exit_code != 0:
        # raise failure
        raise StageOutFailure(stdout)


def move_all_files(files):
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
        exit_code, stdout, stderr = move(source, destination)
        if exit_code != 0:
            logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))
            break

    return exit_code, stdout, stderr


def move(source, destination):
    """
    Tries to upload the given files using mv directly.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'mv', source, destination]
    exit_code, stdout, stderr = execute(executable)

    return exit_code, stdout, stderr
