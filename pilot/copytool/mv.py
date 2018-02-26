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


def copy_in(files, copy_type="mv"):
    """
    Tries to download the given files using mv directly.

    :param files: Files to download
    :raises PilotException: StageInFailure
    """

    if copy_type not in ["cp", "mv", "symlink"]:
        raise StageInFailure("Incorrect method for copy in")
    exit_code, stdout, stderr = move_all_files(files, copy_type)
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)


def copy_out(files, copy_type="mv"):
    """
    Tries to upload the given files using mv directly.

    :param files: Files to upload
    :raises PilotException: StageOutFailure
    """

    if copy_type not in ["cp", "mv"]:
        raise StageOutFailure("Incorrect method for copy out")

    exit_code, stdout, stderr = move_all_files(files, copy_type)
    if exit_code != 0:
        # raise failure
        raise StageOutFailure(stdout)


def move_all_files(files, copy_type):
    """
    Move all files.

    :param files:
    :return: exit_code, stdout, stderr
    """

    exit_code = 0
    stdout = ""
    stderr = ""
    copy_method = None

    if copy_type == "mv":
        copy_method = move
    elif copy_type == "cp":
        copy_method = copy
    elif copy_type == "symlink":
        copy_method = symlink
    else:
        return -1, "", "Incorrect copy method"

    for entry in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}
        logger.info("transferring file %s from %s to %s" % (entry['name'], entry['source'], entry['destination']))

        source = os.path.join(entry['source'], entry['name'])
        destination = os.path.join(entry['destination'], entry['name'])
        exit_code, stdout, stderr = copy_method(source, destination)
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
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr


def copy(source, destination):
    """
    Tries to upload the given files using xrdcp directly.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'cp', source, destination]
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr


def symlink(source, destination):
    """
    Tries to ln the given files.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'ln', '-s', source, destination]
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr
