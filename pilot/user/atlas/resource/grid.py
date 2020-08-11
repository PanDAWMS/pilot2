#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

from pilot.util.container import execute
from pilot.common.errorcodes import ErrorCodes
from ..setup import get_asetup, get_asetup_options

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def verify_setup_command(cmd):
    """
    Verify the setup command.

    :param cmd: command string to be verified (string).
    :return: pilot error code (int), diagnostics (string).
    """

    ec = 0
    diagnostics = ""

    exit_code, stdout, stderr = execute(cmd, timeout=5 * 60)
    if exit_code != 0:
        if "No release candidates found" in stdout:
            logger.info('exit_code=%d' % exit_code)
            logger.info('stdout=%s' % stdout)
            logger.info('stderr=%s' % stderr)
            ec = errors.NORELEASEFOUND
            diagnostics = stdout + stderr

    return ec, diagnostics


def get_setup_command(job, prepareasetup):
    """
    Return the path to asetup command, the asetup command itself and add the options (if desired).
    If prepareasetup is False, the function will only return the path to the asetup script. It is then assumed
    to be part of the job parameters.

    :param job: job object.
    :param prepareasetup: should the pilot prepare the asetup command itself? boolean.
    :return:
    """

    # if cvmfs is not available, assume that asetup is not needed
    # note that there is an exception for sites (BOINC, some HPCs) that have cvmfs but still
    # uses is_cvmfs=False.. these sites do not use containers, so check for that instead
    if job.infosys.queuedata.is_cvmfs or not job.infosys.queuedata.container_type:
        logger.debug('return asetup path as normal since: is_cvmfs=%s, job.container_type=%s' %
                     (job.infosys.queuedata.is_cvmfs, job.infosys.queuedata.container_type))
    else:
        # if not job.infosys.queuedata.is_cvmfs:
        logger.debug('will not return asetup path since: is_cvmfs=%s, job.container_type=%s' %
                     (job.infosys.queuedata.is_cvmfs, job.infosys.queuedata.container_type))
        return ""

    # return immediately if there is no release or if user containers are used
    # if job.swrelease == 'NULL' or (('--containerImage' in job.jobparams or job.imagename) and job.swrelease == 'NULL'):
    if job.swrelease == 'NULL' or job.swrelease == '':
        logger.debug('will not return asetup path since there is no swrelease set')
        return ""

    # Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    cmd = get_asetup(asetup=prepareasetup)

    if prepareasetup:
        options = get_asetup_options(job.swrelease, job.homepackage)
        asetupoptions = " " + options
        if job.platform:
            asetupoptions += " --platform " + job.platform

        # Always set the --makeflags option (to prevent asetup from overwriting it)
        asetupoptions += " --makeflags=\'$MAKEFLAGS\'"

        # Verify that the setup works
        # exitcode, output = timedCommand(cmd, timeout=5 * 60)
        # if exitcode != 0:
        #     if "No release candidates found" in output:
        #         pilotErrorDiag = "No release candidates found"
        #         logger.warning(pilotErrorDiag)
        #         return self.__error.ERR_NORELEASEFOUND, pilotErrorDiag, "", special_setup_cmd, JEM, cmtconfig
        # else:
        #     logger.info("verified setup command")

        cmd += asetupoptions

    return cmd
