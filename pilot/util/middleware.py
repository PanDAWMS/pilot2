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


def containerise_middleware(job, queue, eventtype, localsite, remotesite, label='stage-in'):
    """
    Containerise the middleware by performing stage-in/out steps in a script that in turn can be run in a container.

    :param job: job object.
    :param queue: queue name (string).
    :param eventtype:
    :param localsite:
    :param remotesite:
    :param label: 'stage-in/out' (String).
    :return:
    :raises NotImplemented: if stagein=False, until stage-out script has been written
    :raises StageInFailure: for stage-in failures
    :raises StageOutFailure: for stage-out failures
    """

    cwd = getcwd()
    logger.debug('cwd=%s' % cwd)
    logger.debug('PYTHONPATH=%s' % environ.get('PYTHONPATH'))

    # get the name of the stage-in/out isolation script
    script = config.Container.middleware_container_stagein_script if label == 'stage-in' else config.Container.middleware_container_stageout_script

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
    else:
        logger.warning('bad configuration? check config.Container.use_middleware_container value')

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
            logger.debug('stage-out stdout=\n%s' % stdout)
            logger.debug('stage-out stderr=\n%s' % stderr)
        except PilotException as e:
            msg = 'exception caught: %s' % e
            if label == 'stage-in':
                raise StageInFailure(msg)
            else:
                raise StageOutFailure(msg)

    # handle errors and file statuses (the stage-in/out scripts write errors and file status to a json file)
    try:
        handle_containerised_errors(job, label=label)
    except PilotException as e:
        raise e


def get_script_path(script):
    """

    :param script:
    :return:
    """

    srcdir = environ.get('PILOT_SOURCE_DIR', '.')
    logger.debug('PILOT_SOURCE_DIR=%s' % srcdir)

    _path = path.join(srcdir, 'pilot/scripts')
    if not path.exists(_path):
        _path = path.join(srcdir, 'pilot2')
        _path = path.join(_path, 'pilot/scripts')
    _path = path.join(_path, script)
    if not path.exists(_path):
        _path = ''

    return _path


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
    filedata_dictionary = get_filedata_strings(data)
    srcdir = path.join(environ.get('PILOT_SOURCE_DIR', '.'), 'pilot2')
    if not path.exists(srcdir):
        msg = 'pilot source directory not correct: %s' % srcdir
        logger.debug(msg)
        if label == 'stage-in':
            raise StageInFailure(msg)
        else:
            raise StageOutFailure(msg)
    else:
        logger.debug('using pilot source directory: %s' % srcdir)

    #_path = get_script_path(script)
    #if not path.exists(_path):
    #    msg = 'no such path: %s' % _path
    #    logger.warning(msg)
    #    if label == 'stage-in':
    #        raise StageInFailure(msg)
    #    else:
    #        raise StageOutFailure(msg)

    try:
        pass
        #copy(_path, job.workdir)
    except PilotException as e:
        msg = 'exception caught: %s' % e
        logger.warning(msg)
        if label == 'stage-in':
            raise StageInFailure(msg)
        else:
            raise StageOutFailure(msg)
    else:
        final_script_path = path.join(job.workdir, script)
        #try:
        #    # make the script executable
        #    chmod(final_script_path, 0o755)  # Python 2/3
        #except Exception as e:
        #    msg = 'exception caught: %s' % e
        #    logger.warning(msg)
        #    if label == 'stage-in':
        #        raise StageInFailure(msg)
        #    else:
        #        raise StageOutFailure(msg)

        # copy pilot source into container directory, unless it is already there
        try:
            dest = path.join(job.workdir, 'pilot2')
            if not path.exists(dest):
                logger.debug('copy %s to %s' % (srcdir, dest))
                copytree(srcdir, dest)
            script_path = path.join('pilot2/pilot/scripts', script)
            full_script_path = path.join(path.join(job.workdir, script_path))
            logger.debug('full_script_path=%s' % full_script_path)
            chmod(full_script_path, 0o755)  # Python 2/3
        except Exception as e:
            msg = 'exception caught when copying pilot2 source: %s' % e
            logger.warning(msg)
            if label == 'stage-in':
                raise StageInFailure(msg)
            else:
                raise StageOutFailure(msg)

        if config.Container.use_middleware_container:
            # correct the path when containers have been used
            final_script_path = path.join('.', script_path)  #script)
            workdir = '/srv'
        else:
            workdir = job.workdir

        if label == 'stage-in':
            cmd = '%s --lfns=%s --scopes=%s -w %s -d -q %s --eventtype=%s --localsite=%s ' \
                  '--remotesite=%s --produserid=\"%s\" --jobid=%s --taskid=%s --jobdefinitionid=%s ' \
                  '--eventservicemerge=%s --usepcache=%s' % \
                  (final_script_path, filedata_dictionary['lfns'], filedata_dictionary['scopes'], workdir, queue, eventtype, localsite,
                   remotesite, job.produserid.replace(' ', '%20'), job.jobid, job.taskid, job.jobdefinitionid,
                   job.is_eventservicemerge, job.infosys.queuedata.use_pcache)
        else:  # stage-out
            cmd = '%s --lfns=%s --scopes=%s -w %s -d -q %s --eventtype=%s --localsite=%s ' \
                  '--remotesite=%s --produserid=\"%s\" --jobid=%s --taskid=%s --jobdefinitionid=%s ' \
                  '--datasets=%s --ddmendpoints=%s --guids=%s' % \
                  (final_script_path, filedata_dictionary['lfns'], filedata_dictionary['scopes'], workdir, queue, eventtype, localsite,
                   remotesite, job.produserid.replace(' ', '%20'), job.jobid, job.taskid, job.jobdefinitionid,
                   filedata_dictionary['datasets'], filedata_dictionary['ddmendpoints'], filedata_dictionary['guids'])

    return cmd


def handle_containerised_errors(job, label='stage-in'):
    """

    :param job: job object.
    :param label: 'stage-in/out' (string).
    :return:
    :raises: StageInFailure, StageOutFailure
    """

    dictionary = config.Container.stagein_dictionary if label == 'stage-in' else config.Container.stageout_dictionary
    data = job.indata if label == 'stage-in' else job.outdata

    # read the JSON file created by the stage-in/out script
    if path.exists(path.join(job.workdir, dictionary + '.log')):
        dictionary += '.log'
    file_dictionary = read_json(path.join(job.workdir, dictionary))

    # update the job object accordingly
    if file_dictionary:
        # get file info and set essential parameters
        for fspec in data:
            try:
                fspec.status = file_dictionary[fspec.lfn][0]
                fspec.status_code = file_dictionary[fspec.lfn][1]
                if label == 'stage-out':
                    fspec.surl = file_dictionary[fspec.lfn][2]
                    fspec.turl = file_dictionary[fspec.lfn][3]
                    fspec.checksum[fspec.lfn] = file_dictionary[fspec.lfn][4]
            except Exception as e:
                msg = "exception caught while reading file dictionary: %s" % e
                logger.warning(msg)
                if label == 'stage-in':
                    raise StageInFailure(msg)
                else:
                    raise StageOutFailure(msg)

        # get main error info ('error': [error_diag, error_code])
        error_diag = file_dictionary['error'][0]
        error_code = file_dictionary['error'][1]
        if error_code:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error_code, msg=error_diag)
    else:
        msg = "%s file dictionary not found" % label
        logger.warning(msg)
        if label == 'stage-in':
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
    Return a dictionary with comma-separated list of LFNs, guids, scopes, datasets and ddmendpoints.

    :param data: job [in|out]data (list of FileSpec objects).
    :return: {'lfns': lfns, ..} (dictionary).
    """

    lfns = ""
    guids = ""
    scopes = ""
    datasets = ""
    ddmendpoints = ""
    for fspec in data:
        lfns = fspec.lfn if lfns == "" else lfns + ",%s" % fspec.lfn
        guids = fspec.guid if lfns == "" else guids + ",%s" % fspec.guid
        scopes = fspec.scope if scopes == "" else scopes + ",%s" % fspec.scope
        datasets = fspec.dataset if datasets == "" else datasets + ",%s" % fspec.dataset
        ddmendpoints = fspec.ddmendpoint if ddmendpoints == "" else ddmendpoints + ",%s" % fspec.ddmendpoint

    return {'lfns': lfns, 'guids': guids, 'scopes': scopes, 'datasets': datasets, 'ddmendpoints': ddmendpoints}


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
