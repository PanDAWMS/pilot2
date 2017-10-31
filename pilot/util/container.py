#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import subprocess
from os import environ

import logging
logger = logging.getLogger(__name__)


def execute(executable, **kwargs):
    """
    Execute the command and its options in the provided executable list.
    The function also determines whether the command should be executed within a container.
    TODO: add time-out functionality.

    :param executable: Command list to be executed.
    :param kwargs:
    :return: exit code, stdout and stderr
    """

    timeout = kwargs.get('timeout', 120)
    usecontainer = kwargs.get('usecontainer', False)

    # Import user specific code in case
    if usecontainer:
        logger.info("will use container")
        user = environ.get('PILOT_USER', 'generic')  # TODO: replace with singleton
        container = __import__('pilot.user.%s.container' % user, globals(), locals(), [user], -1)
        if container:
            executable = container.wrapper(executable, kwargs=**kwargs)
    else:
        logger.info("will not use container")

    logger.info('executing command: %s' % executable)
    process = subprocess.Popen(executable,
                               bufsize=-1,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               shell=True)
    stdout, stderr = process.communicate()
    exit_code = process.poll()

    return exit_code, stdout, stderr
