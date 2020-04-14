#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import subprocess
from os import environ, getcwd, setpgrp, path, chmod  #, getpgid  #setsid
from sys import version_info

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException, NotImplemented, StageInFailure, StageOutFailure
from pilot.util.config import config
from pilot.util.filehandling import copy, read_json, write_file  #, find_executable

import logging
logger = logging.getLogger(__name__)
errors = ErrorCodes()


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
        exe = ['/usr/bin/python'] + executable.split()
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


def containerise_middleware(job, queue, script, eventtype, localsite, remotesite, stagein=True):
    """
    Containerise the middleware by performing stage-in/out steps in a script that in turn can be run in a container.

    :param job: job object.
    :param queue: queue name (string).
    :param script: full path to stage-in/out script (string).
    :param eventtype:
    :param localsite:
    :param remotesite:
    :param stagein: optional Boolean.
    :return:
    :raises NotImplemented: if stagein=False, until stage-out script has been written
    :raises StageInFailure: for stage-in failures
    :raises StageOutFailure: for stage-out failures
    """

    try:
        if stagein:
            cmd = get_stagein_command(job, queue, script, eventtype, localsite, remotesite)
        else:
            raise NotImplemented("stage-out script not implemented yet")
    except PilotException as e:
        raise e

    usecontainer = config.Container.use_middleware_container
    #mode = 'python' if not usecontainer else ''
    try:
        exit_code, stdout, stderr = execute(cmd, job=job, usecontainer=usecontainer)
    except Exception as e:
        logger.warning('exception caught: %s' % e)
    else:
        logger.debug('stage-in script returned exit_code=%d' % exit_code)

        # write stdout+stderr to files
        try:
            stagein_stdout, stagein_stderr = get_stagein_logfile_names()
            write_file(path.join(job.workdir, stagein_stdout), stdout, mute=False)
            write_file(path.join(job.workdir, stagein_stderr), stderr, mute=False)
        except PilotException as e:
            msg = 'exception caught: %s' % e
            if stagein:
                raise StageInFailure(msg)
            else:
                raise StageOutFailure(msg)

    # handle errors and file statuses (the stage-in script writes errors and file status to a json file)
    try:
        handle_containerised_errors(job)
    except PilotException as e:
        raise e


def get_stagein_command(job, queue, script, eventtype, localsite, remotesite):
    """

    :param job: job object.
    :param queue: queue name (string).
    :param script:
    :param eventtype:
    :param localsite:
    :param remotesite:
    :return: stage-in command (string).
    :raises StageInFailure: for stage-in failures
    """

    try:
        lfns, scopes = get_filedata_strings(job.indata)
        srcdir = path.join(environ.get('PILOT_SOURCE_DIR'), 'pilot2')
        copy(path.join(path.join(srcdir, 'pilot/scripts'), script), srcdir)
        final_script_path = path.join(srcdir, script)
        chmod(final_script_path, 0o755)  # Python 2/3
    except PilotException as e:
        msg = 'exception caught: %s' % e
        logger.warning(msg)
        raise StageInFailure(msg)
    else:
        cmd = '%s --lfns=%s --scopes=%s -w %s -d -q %s --eventtype=%s --localsite=%s ' \
              '--remotesite=%s --produserid=\"%s\" --jobid=%s --taskid=%s --jobdefinitionid=%s ' \
              '--eventservicemerge=%s --usepcache=%s' % \
              (final_script_path, lfns, scopes, job.workdir, queue, eventtype, localsite,
               remotesite, job.produserid.replace(' ', '%20'), job.jobid, job.taskid, job.jobdefinitionid,
               job.is_eventservicemerge, job.infosys.queuedata.use_pcache)

    return cmd


def handle_containerised_errors(job, stagein=True):
    """

    :param job: job object.
    :param stagein: Boolean.
    :return:
    :raises: StageInFailure, StageOutFailure
    """

    # read the JSON file created by the stage-in/out script
    file_dictionary = read_json(path.join(job.workdir, config.Container.stagein_dictionary))

    # update the job object accordingly
    if file_dictionary:
        # get file info
        for fspec in job.indata:
            try:
                fspec.status = file_dictionary[fspec.lfn][0]
                fspec.status_code = file_dictionary[fspec.lfn][1]
            except Exception as e:
                msg = "exception caught while reading file dictionary: %s" % e
                logger.warning(msg)
                if stagein:
                    raise StageInFailure(msg)
                else:
                    raise StageOutFailure(msg)
        # get main error info ('error': [error_diag, error_code])
        error_diag = file_dictionary['error'][0]
        error_code = file_dictionary['error'][1]
        if error_code:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error_code, msg=error_diag)
    else:
        msg = "stage-in file dictionary not found"
        logger.warning(msg)
        if stagein:
            raise StageInFailure(msg)
        else:
            raise StageOutFailure(msg)


def get_stagein_logfile_names():
    """
    Get the proper names for the redirected stage-in logs.

    :return: stagein_stdout (string), stagein_stderr (string).
    """

    stagein_stdout = config.Container.middleware_stagein_stdout
    if not stagein_stdout:
        stagein_stdout = 'stagein_stdout.txt'
    stagein_stderr = config.Container.middleware_stagein_stderr
    if not stagein_stderr:
        stagein_stderr = 'stagein_stderr.txt'

    return stagein_stdout, stagein_stderr


def get_filedata_strings(indata):
    """
    Return a comma-separated list of LFNs and scopes.

    :param indata: job indata (list of FileSpec).
    :return: lfns (string), scopes (string).
    """

    lfns = ""
    scopes = ""
    for fspec in indata:
        lfns = fspec.lfn if lfns == "" else lfns + ",%s" % fspec.lfn
        scopes = fspec.scope if scopes == "" else scopes + ",%s" % fspec.scope

    return lfns, scopes


def use_middleware_container(cmd):
    """
    Should the pilot use a container for the stage-in/out?

    :param cmd: middleware command, used to determine if the container should be used or not (string).
    :return: Boolean.
    """

    # currently not used

    # see definition in atlas/container.py, but also see useful code below (in case middleware is available locally)

    #usecontainer = False
    #if not config.Container.middleware_container:
    #    logger.info('container usage for middleware is not allowed by pilot config')
    #else:
    #    # if the middleware is available locally, do not use container
    #    if find_executable(cmd) == "":
    #        usecontainer = True
    #        logger.info('command %s is not available locally, will attempt to use container' % cmd)
    #    else:
    #        logger.info('command %s is available locally, no need to use container' % cmd)

    return False  #usecontainer
