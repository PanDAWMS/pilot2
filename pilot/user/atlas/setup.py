#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os

from pilot.util.information import get_parameter

import logging
logger = logging.getLogger(__name__)


def get_file_system_root_path():
    """
    Return the root path of the local file system.
    The function returns "/cvmfs" or "/(some path)/cvmfs" in case the expected file system root path is not
    where it usually is (e.g. on an HPC). A site can set the base path by exporting ATLAS_SW_BASE.

    :return: path (string)
    """

    return os.environ.get('ATLAS_SW_BASE', '/cvmfs')


def should_pilot_prepare_asetup(noExecStrCnv, jobPars):
    """
    Determine whether the pilot should add the asetup to the payload command or not.
    The pilot will not add asetup if jobPars already contain the information (i.e. it was set by the payload creator).
    If noExecStrCnv is set, then jobPars is expected to contain asetup.sh + options

    :param noExecStrCnv: boolean
    :param jobPars: string
    :return: boolean
    """

    prepareasetup = True
    if noExecStrCnv:
        if "asetup.sh" in jobPars:
            logger.info("asetup will be taken from jobPars")
            prepareasetup = False
        else:
            logger.info("noExecStrCnv is set but asetup command was not found in jobPars (pilot will prepare asetup)")
            prepareasetup = True
    else:
        logger.info("pilot will prepare asetup")
        prepareasetup = True

    return prepareasetup


def is_user_analysis_job(trf):
    """
    Determine whether the job is an analysis job or not.
    The trf name begins with a protocol for user analysis jobs.

    :param trf:
    :return:
    """

    if (trf.startswith('https://') or trf.startswith('http://')):
        analysisjob = True
    else:
        analysisjob = False

    return analysisjob


def get_cmtconfig(jobcmtconfig, queuedata):
    """
    Get the cmtconfig from the job def or schedconfig

    :param jobcmtconfig: platform information from the job definition (string).
    :param queuedata: the queuedata dictionary from schedconfig.
    :return: chosen platform (string).
    """

    # the job def should always contain the cmtconfig
    if jobcmtconfig != "" and jobcmtconfig != "None" and jobcmtconfig != "NULL":
        cmtconfig = jobcmtconfig
        logger.info("Will try to use cmtconfig: %s (from job definition)" % cmtconfig)
    else:
        cmtconfig = get_parameter(queuedata, 'cmtconfig')
        logger.info("Will try to use cmtconfig: %s (from schedconfig DB)" % cmtconfig)

    return cmtconfig
