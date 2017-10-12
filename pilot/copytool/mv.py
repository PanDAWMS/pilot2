#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import subprocess
import os

from pilot.common.exception import PilotException

import logging

logger = logging.getLogger(__name__)


def copy_in(files):
    """
    Tries to download the given files using mv directly.

    :param files: Files to download

    :raises Exception
    """

    for entry in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}
        logger.info("Transferring file %s from %s to %s" % (entry['name'], entry['source'], entry['destination']))

        source = os.path.join(entry['source'], entry['name'])
        destination = os.path.join(entry['destination'], entry['name'])
        exit_code, stdout, stderr = move(source, destination)
        if exit_code != 0:
            logger.warning("Transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stdout))
            # raise failure
            # raise PilotException


def copy_out(files):
    """
    Tries to upload the given files using mv directly.

    :param files: Files to upload

    :raises Exception
    """

    # same workflow as copy_in, so reuse it
    try:
        copy_in(files)
    except PilotException as e:
        logger.warning("Caught exception: %s" % e)
        raise e


def move(source, destination):
    """
    Tries to upload the given files using xrdcp directly.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'mv', source, destination]
    process = subprocess.Popen(executable,
                               bufsize=-1,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    exit_code = process.poll()

    return exit_code, stdout, stderr
