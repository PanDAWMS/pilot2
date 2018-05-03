#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import subprocess
from os import environ, getcwd, setsid

import logging
logger = logging.getLogger(__name__)


def execute(executable, **kwargs):
    """
    Execute the command and its options in the provided executable list.
    The function also determines whether the command should be executed within a container.
    TODO: add time-out functionality.

    :param executable: command to be executed (string or list).
    :param kwargs (timeout, usecontainer, returnproc):
    :return: exit code, stdout and stderr (or process if requested via returnproc argument)
    """

    cwd = kwargs.get('cwd', getcwd())
    stdout = kwargs.get('stdout', subprocess.PIPE)
    stderr = kwargs.get('stderr', subprocess.PIPE)
    timeout = kwargs.get('timeout', 120)
    usecontainer = kwargs.get('usecontainer', False)
    returnproc = kwargs.get('returnproc', False)
    mute = kwargs.get('mute', False)
    job = kwargs.get('job')

    # convert executable to string if it is a list
    if type(executable) is list:
        executable = ' '.join(executable)

    # Import user specific code if necessary (in case the command should be executed in a container)
    # Note: the container.wrapper() function must at least be declared
    if usecontainer:
        user = environ.get('PILOT_USER', 'generic').lower()  # TODO: replace with singleton
        container = __import__('pilot.user.%s.container' % user, globals(), locals(), [user], -1)
        if container:
            try:
                executable = container.wrapper(executable, **kwargs)
            except Exception as e:
                logger.fatal('failed to execute wrapper function: %s' % e)
    else:
        # logger.info("will not use container")
        pass

    if not mute:
        logger.info('executing command: %s' % executable)
    exe = ['/bin/bash', '-c', executable]
    process = subprocess.Popen(exe,
                               bufsize=-1,
                               stdout=stdout,
                               stderr=stderr,
                               cwd=cwd,
                               preexec_fn=setsid)
    if returnproc:
        return process
    else:
        stdout, stderr = process.communicate()
        exit_code = process.poll()

        return exit_code, stdout, stderr
