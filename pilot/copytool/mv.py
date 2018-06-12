#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2018

import os

from pilot.common.exception import StageInFailure, StageOutFailure, ErrorCodes
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)

require_replicas = False  ## indicate if given copytool requires input replicas to be resolved


def is_valid_for_copy_in(files):
    for f in files:
        if not all(key in f for key in ('name', 'source', 'destination')):
            return False
    return True


def is_valid_for_copy_out(files):
    for f in files:
        if not all(key in f for key in ('name', 'source', 'destination')):
            return False
    return True


def copy_in(files, copy_type="mv", **kwargs):
    """
    Tries to download the given files using mv directly.

    :param files: list of `FileSpec` objects
    :raises PilotException: StageInFailure
    """

    if copy_type not in ["cp", "mv", "symlink"]:
        raise StageInFailure("Incorrect method for copy in")
    exit_code, stdout, stderr = move_all_files(files, copy_type, kwargs)
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)


def copy_out(files, copy_type="mv", **kwargs):
    """
    Tries to upload the given files using mv directly.

    :param files: list of `FileSpec` objects
    :raises PilotException: StageOutFailure
    """

    if copy_type not in ["cp", "mv"]:
        raise StageOutFailure("Incorrect method for copy out")

    exit_code, stdout, stderr = move_all_files(files, copy_type, kwargs)
    if exit_code != 0:
        # raise failure
        raise StageOutFailure(stdout)


def move_all_files(files, copy_type, **kwargs):
    """
    Move all files.

    :param files: list of `FileSpec` objects
    :return: exit_code, stdout, stderr
    """

    exit_code = 0
    stdout = ""
    stderr = ""
    # copy_method = None

    if copy_type == "mv":
        copy_method = move
    elif copy_type == "cp":
        copy_method = copy
    elif copy_type == "symlink":
        copy_method = symlink
    else:
        return -1, "", "Incorrect copy method"

    for fspec in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        #dst = fspec.workdir or '.'
        #timeout = get_timeout(fspec.filesize)
        source = fspec.turl
        name = fspec.lfn
        destination = os.path.join(dst, name)

        logger.info("transferring file %s from %s to %s" % (name, source, destination))

        source = os.path.join(source, name)
        destination = os.path.join(destination, name)
        exit_code, stdout, stderr = copy_method(source, destination)
        if exit_code != 0:
            logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))
            fspec.status = 'failed'
            fspec.status_code = ErrorCodes.STAGEOUTFAILED  # to fix, what about stage-in?
            break
        else:
            fspec.status_code = 0
            fspec.status = 'transferred'

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
