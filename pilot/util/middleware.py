#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

from os import environ, path, chmod, getcwd
from shutil import copytree

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException, NotImplemented, StageInFailure, StageOutFailure
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import copy, read_json, write_file  #, find_executable

import logging
logger = logging.getLogger(__name__)
errors = ErrorCodes()


def containerise_middleware(job, queue, eventtype, localsite, remotesite, stagein=True):
    """
    Containerise the middleware by performing stage-in/out steps in a script that in turn can be run in a container.

    :param job: job object.
    :param queue: queue name (string).
    :param eventtype:
    :param localsite:
    :param remotesite:
    :param stagein: optional Boolean.
    :return:
    :raises NotImplemented: if stagein=False, until stage-out script has been written
    :raises StageInFailure: for stage-in failures
    :raises StageOutFailure: for stage-out failures
    """

    cwd = getcwd()

    # get the name of the stage-in isolation script
    script = config.Container.middleware_container_stagein_script

    label = 'stage-in' if stagein else 'stage-out'
    try:
        cmd = get_command(job, queue, script, eventtype, localsite, remotesite, label=label)
    except PilotException as e:
        raise e

    if config.Container.use_middleware_container:
        # add bits and pieces needed to run the cmd in a container
        pilot_user = environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.container' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
        try:
            cmd = user.create_middleware_container_command(job.workdir, cmd, label=label)
        except PilotException as e:
            raise e

    try:
        logger.info('*** executing %s (logging will be redirected) ***' % label)
        exit_code, stdout, stderr = execute(cmd, job=job, usecontainer=False)
    except Exception as e:
        logger.info('*** %s has failed ***' % label)
        logger.warning('exception caught: %s' % e)
    else:
        if exit_code == 0:
            logger.info('*** %s has finished ***' % label)
        else:
            logger.info('*** %s has failed ***' % label)
        logger.debug('%s script returned exit_code=%d' % (label, exit_code))

        # write stdout+stderr to files
        try:
            _stdout_name, _stderr_name = get_logfile_names(label)
            write_file(path.join(job.workdir, _stdout_name), stdout, mute=False)
            write_file(path.join(job.workdir, _stderr_name), stderr, mute=False)
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


def get_command(job, queue, script, eventtype, localsite, remotesite, label='stage-in'):
    """

    :param job: job object.
    :param queue: queue name (string).
    :param script: name of stage-in/out script (string).
    :param eventtype:
    :param localsite:
    :param remotesite:
    :param label: 'stage-[in|out]' (string).
    :return: stage-in/out command (string).
    :raises StageInFailure: for stage-in failures
    :raises StageOutFailure: for stage-out failures
    """

    data = job.indata if label == 'stage-in' else job.outdata
    lfns, scopes = get_filedata_strings(data)
    srcdir = environ.get('PILOT_SOURCE_DIR', '.')

    try:
        copy(path.join(path.join(srcdir, 'pilot/scripts'), script), job.workdir)
    except PilotException as e:
        msg = 'exception caught: %s' % e
        logger.warning(msg)
        if label == 'stage-in':
            raise StageInFailure(msg)
        else:
            raise StageOutFailure(msg)
    else:
        final_script_path = path.join(job.workdir, script)
        try:
            # make the script executable
            chmod(final_script_path, 0o755)  # Python 2/3
        except Exception as e:
            msg = 'exception caught: %s' % e
            logger.warning(msg)
            if label == 'stage-in':
                raise StageInFailure(msg)
            else:
                raise StageOutFailure(msg)

        # copy pilot source for now - investigate why there is a config read error when source is set to cvmfs pilot dir
        try:
            copytree(srcdir, path.join(job.workdir, 'pilot2'))
        except Exception as e:
            msg = 'exception caught when copying pilot2 source: %s' % e
            logger.warning(msg)
            if label == 'stage-in':
                raise StageInFailure(msg)
            else:
                raise StageOutFailure(msg)

        if config.Container.use_middleware_container:
            # correct the path when containers have been used
            final_script_path = path.join('.', script)
            workdir = '/srv'
        else:
            workdir = job.workdir

        if label == 'stage-in':
            cmd = '%s --lfns=%s --scopes=%s -w %s -d -q %s --eventtype=%s --localsite=%s ' \
                  '--remotesite=%s --produserid=\"%s\" --jobid=%s --taskid=%s --jobdefinitionid=%s ' \
                  '--eventservicemerge=%s --usepcache=%s' % \
                  (final_script_path, lfns, scopes, workdir, queue, eventtype, localsite,
                   remotesite, job.produserid.replace(' ', '%20'), job.jobid, job.taskid, job.jobdefinitionid,
                   job.is_eventservicemerge, job.infosys.queuedata.use_pcache)
        else:
            raise NotImplemented("stage-out script not implemented yet")

    return cmd


def handle_containerised_errors(job, stagein=True):
    """

    :param job: job object.
    :param stagein: Boolean.
    :return:
    :raises: StageInFailure, StageOutFailure
    """

    # read the JSON file created by the stage-in/out script
    _path = path.join(job.workdir)
    file_dictionary = read_json(path.join(_path, config.Container.stagein_dictionary))
    if not file_dictionary:
        _path = path.join(_path, '..')
        file_dictionary = read_json(path.join(_path, config.Container.stagein_dictionary))

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


def get_logfile_names(label):
    """
    Get the proper names for the redirected stage-in/out logs.

    :param label: 'stage-[in|out]' (string)
    :return: 'stage[in|out]_stdout' (string), 'stage[in|out]_stderr' (string).
    """

    if label == 'stage-in':
        _stdout_name = config.Container.middleware_stagein_stdout
        _stderr_name = config.Container.middleware_stagein_stderr
    else:
        _stdout_name = config.Container.middleware_stageout_stdout
        _stderr_name = config.Container.middleware_stageout_stderr
    if not _stdout_name:
        _stdout_name = 'stagein_stdout.txt' if label == 'stage-in' else 'stageout_stdout.txt'
    if not _stderr_name:
        _stderr_name = 'stagein_stderr.txt' if label == 'stage-in' else 'stageout_stderr.txt'

    return _stdout_name, _stderr_name


def get_filedata_strings(data):
    """
    Return a comma-separated list of LFNs and scopes.

    :param data: job [in|out]data (list of FileSpec).
    :return: lfns (string), scopes (string).
    """

    lfns = ""
    scopes = ""
    for fspec in data:
        lfns = fspec.lfn if lfns == "" else lfns + ",%s" % fspec.lfn
        scopes = fspec.scope if scopes == "" else scopes + ",%s" % fspec.scope

    return lfns, scopes


def use_middleware_container(container_type):
    """
    Should the pilot use a container for the stage-in/out?
    Check the container_type (from queuedata) if 'middleware' is set to 'container'.

    :param container_type: container type (string).
    :return: Boolean (True if middleware should be containerised).
    """

    # see definition in atlas/container.py, but also see useful code below (in case middleware is available locally)
    #:param cmd: middleware command, used to determine if the container should be used or not (string).
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

    # FOR TESTING
    #return True if config.Container.middleware_container_stagein_script else False
    return True if container_type == 'container' else False
