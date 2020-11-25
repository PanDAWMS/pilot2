#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2020

import os
from signal import SIGTERM

from pilot.common.exception import TrfDownloadFailure
from pilot.util.config import config
from pilot.util.constants import UTILITY_BEFORE_PAYLOAD, UTILITY_AFTER_PAYLOAD
from pilot.util.filehandling import read_file
from .setup import get_analysis_trf

import logging
logger = logging.getLogger(__name__)


def sanity_check():
    """
    Perform an initial sanity check before doing anything else in a given workflow.
    This function can be used to verify importing of modules that are otherwise used much later, but it is better to abort
    the pilot if a problem is discovered early.

    :return: exit code (0 if all is ok, otherwise non-zero exit code).
    """

    return 0


def validate(job):
    """
    Perform user specific payload/job validation.

    :param job: job object.
    :return: Boolean (True if validation is successful).
    """

    return True


def get_payload_command(job):
    """
    Return the full command for executing the payload, including the sourcing of all setup files and setting of
    environment variables.

    By default, the full payload command is assumed to be in the job.jobparams.

    :param job: job object
    :return: command (string)
    """

    # Try to download the trf
    # if job.imagename != "" or "--containerImage" in job.jobparams:
    #    job.transformation = os.path.join(os.path.dirname(job.transformation), "runcontainer")
    #    logger.warning('overwrote job.transformation, now set to: %s' % job.transformation)
    ec, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir)
    if ec != 0:
        raise TrfDownloadFailure(diagnostics)
    else:
        logger.debug('user analysis trf: %s' % trf_name)

    return get_analysis_run_command(job, trf_name)


def get_analysis_run_command(job, trf_name):
    """
    Return the proper run command for the user job.

    Example output: export X509_USER_PROXY=<..>;./runAthena <job parameters> --usePFCTurl --directIn

    :param job: job object.
    :param trf_name: name of the transform that will run the job (string). Used when containers are not used.
    :return: command (string).
    """

    cmd = ""

    # add the user proxy
    if 'X509_USER_PROXY' in os.environ and not job.imagename:
        cmd += 'export X509_USER_PROXY=%s;' % os.environ.get('X509_USER_PROXY')

    # set up trfs
    if job.imagename == "":  # user jobs with no imagename defined
        cmd += './%s %s' % (trf_name, job.jobparams)
    else:
        if trf_name:
            cmd += './%s %s' % (trf_name, job.jobparams)
        else:
            cmd += 'python %s %s' % (trf_name, job.jobparams)

    return cmd


def update_job_data(job):
    """
    This function can be used to update/add data to the job object.
    E.g. user specific information can be extracted from other job object fields. In the case of ATLAS, information
    is extracted from the metaData field and added to other job object fields.

    :param job: job object
    :return:
    """

    pass


def remove_redundant_files(workdir, outputfiles=[], islooping=False):
    """
    Remove redundant files and directories prior to creating the log file.

    :param workdir: working directory (string).
    :param outputfiles: list of output files.
    :param islooping: looping job variable to make sure workDir is not removed in case of looping (boolean).
    :return:
    """

    pass


def get_utility_commands(order=None, job=None):
    """
    Return a dictionary of utility commands and arguments to be executed in parallel with the payload.
    This could e.g. be memory and network monitor commands. A separate function can be used to determine the
    corresponding command setups using the utility command name.
    If the optional order parameter is set, the function should return the list of corresponding commands.
    E.g. if order=UTILITY_BEFORE_PAYLOAD, the function should return all commands that are to be executed before the
    payload. If order=UTILITY_WITH_PAYLOAD, the corresponding commands will be prepended to the payload execution
    string. If order=UTILITY_AFTER_PAYLOAD, the commands that should be executed after the payload has been started
    should be returned.

    FORMAT: {'command': <command>, 'args': <args>}

    :param order: optional sorting order (see pilot.util.constants)
    :param job: optional job object.
    :return: dictionary of utilities to be executed in parallel with the payload.
    """

    return {}


def get_utility_command_setup(name, setup=None):
    """
    Return the proper setup for the given utility command.
    If a payload setup is specified
    :param name:
    :param setup:
    :return:
    """

    pass


def get_utility_command_execution_order(name):
    """
    Should the given utility command be executed before or after the payload?

    :param name: utility name (string).
    :return: execution order constant (UTILITY_BEFORE_PAYLOAD or UTILITY_AFTER_PAYLOAD)
    """

    # example implementation
    if name == 'monitor':
        return UTILITY_BEFORE_PAYLOAD
    else:
        return UTILITY_AFTER_PAYLOAD


def post_utility_command_action(name, job):
    """
    Perform post action for given utility command.

    :param name: name of utility command (string).
    :param job: job object.
    :return:
    """

    pass


def get_utility_command_kill_signal(name):
    """
    Return the proper kill signal used to stop the utility command.

    :param name:
    :return: kill signal
    """

    return SIGTERM


def get_utility_command_output_filename(name, selector=None):
    """
    Return the filename to the output of the utility command.

    :param name: utility name (string).
    :param selector: optional special conditions flag (boolean).
    :return: filename (string).
    """

    return ""


def verify_job(job):
    """
    Verify job parameters for specific errors.
    Note:
      in case of problem, the function should set the corresponding pilot error code using
      job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())

    :param job: job object
    :return: Boolean.
    """

    return True


def update_stagein(job):
    """
    In case special files need to be skipped during stage-in, the job.indata list can be updated here.
    See ATLAS code for an example.

    :param job: job object.
    :return:
    """

    pass


def get_metadata(workdir):
    """
    Return the metadata from file.

    :param workdir: work directory (string)
    :return:
    """

    path = os.path.join(workdir, config.Payload.jobreport)
    metadata = read_file(path) if os.path.exists(path) else None

    return metadata


def update_server(job):
    """
    Perform any user specific server actions.

    E.g. this can be used to send special information to a logstash.

    :param job: job object.
    :return:
    """

    pass
