#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import subprocess
from os import environ, getcwd, setpgrp  #, getpgid  #setsid
from sys import version_info

import logging
logger = logging.getLogger(__name__)


def is_python3():
    """
    Check if we are running on Python 3.

    :return: boolean.
    """

    return version_info >= (3, 0)


def execute(executable, **kwargs):  # noqa: C901
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
    timeout = kwargs.get('timeout', None)
    usecontainer = kwargs.get('usecontainer', False)
    returnproc = kwargs.get('returnproc', False)
    mute = kwargs.get('mute', False)
    job = kwargs.get('job')
    mode = kwargs.get('mode', 'bash')

    # convert executable to string if it is a list
    if type(executable) is list:
        executable = ' '.join(executable)

    # switch off pilot controlled containers for user defined containers
    if job and job.imagename != "" and "runcontainer" in executable:
        usecontainer = False
        job.usecontainer = usecontainer

    # Import user specific code if necessary (in case the command should be executed in a container)
    # Note: the container.wrapper() function must at least be declared
    if usecontainer:
        user = environ.get('PILOT_USER', 'generic').lower()  # TODO: replace with singleton
        container = __import__('pilot.user.%s.container' % user, globals(), locals(), [user], 0)  # Python 2/3
        if container:
            # should a container really be used?
            do_use_container = job.usecontainer if job else container.do_use_container(**kwargs)

            if do_use_container:
                diagnostics = ""
                try:
                    executable = container.wrapper(executable, **kwargs)
                except Exception as e:
                    diagnostics = 'failed to execute wrapper function: %s' % e
                    logger.fatal(diagnostics)
                else:
                    if executable == "":
                        diagnostics = 'failed to prepare container command'
                        logger.fatal(diagnostics)
                if diagnostics != "":
                    return None if returnproc else -1, "", diagnostics
            else:
                logger.info('pilot user container module has decided to not use a container')
        else:
            logger.warning('container module could not be imported')

    if not mute:
        executable_readable = executable
        executables = executable_readable.split(";")
        for sub_cmd in executables:
            if 'S3_SECRET_KEY=' in sub_cmd:
                secret_key = sub_cmd.split('S3_SECRET_KEY=')[1]
                secret_key = 'S3_SECRET_KEY=' + secret_key
                executable_readable = executable_readable.replace(secret_key, 'S3_SECRET_KEY=********')
        logger.info('executing command: %s' % executable_readable)

    if mode == 'python':
        exe = ['/usr/bin/python', executable]
    else:
        exe = ['/bin/bash', '-c', executable]

    # try: intercept exception such as OSError -> report e.g. error.RESOURCEUNAVAILABLE: "Resource temporarily unavailable"
    process = subprocess.Popen(exe,
                               bufsize=-1,
                               stdout=stdout,
                               stderr=stderr,
                               cwd=cwd,
                               preexec_fn=setpgrp)  #setsid)
    if returnproc:
        return process
    else:
        stdout, stderr = process.communicate()
        exit_code = process.poll()

        # for Python 3, convert from byte-like object to str
        if is_python3():
            stdout = stdout.decode('utf-8')
            stderr = stderr.decode('utf-8')
        # remove any added \n
        if stdout and stdout.endswith('\n'):
            stdout = stdout[:-1]

        return exit_code, stdout, stderr
