#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2021

import subprocess
from os import environ, getcwd, setpgrp  #, getpgid  #setsid
from sys import version_info

from pilot.common.errorcodes import ErrorCodes

import logging
logger = logging.getLogger(__name__)
errors = ErrorCodes()


def is_python3():
    """
    Check if we are running on Python 3.

    :return: boolean.
    """

    return version_info >= (3, 0)


def execute(executable, **kwargs):
    """
    Execute the command and its options in the provided executable list.
    The function also determines whether the command should be executed within a container.

    :param executable: command to be executed (string or list).
    :param kwargs (timeout, usecontainer, returnproc):
    :return: exit code, stdout and stderr (or process if requested via returnproc argument)
    """

    mute = kwargs.get('mute', False)
    mode = kwargs.get('mode', 'bash')
    cwd = kwargs.get('cwd', getcwd())
    stdout_name = kwargs.get('stdout', subprocess.PIPE)
    stderr_name = kwargs.get('stderr', subprocess.PIPE)
    usecontainer = kwargs.get('usecontainer', False)
    returnproc = kwargs.get('returnproc', False)
    # timeout = kwargs.get('timeout', None)  # Python 3
    job = kwargs.get('job')

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
        executable, diagnostics = containerise_executable(executable, **kwargs)
        if not executable:
            return None if returnproc else -1, "", diagnostics

    if not mute:
        executable_readable = executable
        executables = executable_readable.split(";")
        for sub_cmd in executables:
            if 'S3_SECRET_KEY=' in sub_cmd:
                secret_key = sub_cmd.split('S3_SECRET_KEY=')[1]
                secret_key = 'S3_SECRET_KEY=' + secret_key
                executable_readable = executable_readable.replace(secret_key, 'S3_SECRET_KEY=********')
        logger.info('executing command: %s', executable_readable)

    if mode == 'python':
        exe = ['/usr/bin/python'] + executable.split()
    else:
        exe = ['/bin/bash', '-c', executable]

    # try: intercept exception such as OSError -> report e.g. error.RESOURCEUNAVAILABLE: "Resource temporarily unavailable"
    if is_python3():  # Python 3
        process = subprocess.Popen(exe,
                                   bufsize=-1,
                                   stdout=stdout_name,
                                   stderr=stderr_name,
                                   cwd=cwd,
                                   preexec_fn=setpgrp,
                                   encoding='utf-8',
                                   errors='replace')
    else:
        process = subprocess.Popen(exe,
                                   bufsize=-1,
                                   stdout=stdout_name,
                                   stderr=stderr_name,
                                   cwd=cwd,
                                   preexec_fn=setpgrp)
    if returnproc:
        return process
    else:
        stdout, stderr = process.communicate()
        exit_code = process.poll()

        # remove any added \n
        if stdout and stdout.endswith('\n'):
            stdout = stdout[:-1]

    return exit_code, stdout, stderr


def containerise_executable(executable, **kwargs):
    """
    Wrap the containerisation command around the executable.

    :param executable: command to be wrapper (string).
    :param kwargs: kwargs dictionary.
    :return: containerised executable (list).
    """

    job = kwargs.get('job')

    user = environ.get('PILOT_USER', 'generic').lower()  # TODO: replace with singleton
    container = __import__('pilot.user.%s.container' % user, globals(), locals(), [user], 0)  # Python 2/3
    if container:
        # should a container really be used?
        do_use_container = job.usecontainer if job else container.do_use_container(**kwargs)
        # overrule for event service
        if job and job.is_eventservice and do_use_container and environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') != 'raythena':
            logger.info('overruling container decision for event service grid job')
            do_use_container = False

        if do_use_container:
            diagnostics = ""
            try:
                executable = container.wrapper(executable, **kwargs)
            except Exception as exc:
                diagnostics = 'failed to execute wrapper function: %s' % exc
                logger.fatal(diagnostics)
            else:
                if executable == "":
                    diagnostics = 'failed to prepare container command (error code should have been set)'
                    logger.fatal(diagnostics)
            if diagnostics != "":
                return None, diagnostics
        else:
            logger.info('pilot user container module has decided to not use a container')
    else:
        logger.warning('container module could not be imported')

    return executable, ""
