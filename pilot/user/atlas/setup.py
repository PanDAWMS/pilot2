#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os
import re

from pilot.util.information import get_cmtconfig, get_appdir

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


def should_pilot_prepare_asetup(noexecstrcnv, jobpars):
    """
    Determine whether the pilot should add the asetup to the payload command or not.
    The pilot will not add asetup if jobPars already contain the information (i.e. it was set by the payload creator).
    If noExecStrCnv is set, then jobPars is expected to contain asetup.sh + options

    :param noexecstrcnv: boolean
    :param jobpars: string
    :return: boolean
    """

    prepareasetup = True
    if noexecstrcnv:
        if "asetup.sh" in jobpars:
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


def get_platform(jobcmtconfig):
    """
    Get the platform (cmtconfig) from the job def or schedconfig

    :param jobcmtconfig: platform information from the job definition (string).
    :return: chosen platform (string).
    """

    # the job def should always contain the cmtconfig
    if jobcmtconfig != "" and jobcmtconfig != "None" and jobcmtconfig != "NULL":
        cmtconfig = jobcmtconfig
        logger.info("Will try to use cmtconfig: %s (from job definition)" % cmtconfig)
    else:
        cmtconfig = get_cmtconfig()
        logger.info("Will try to use cmtconfig: %s (from schedconfig DB)" % cmtconfig)

    return cmtconfig


def get_asetup(asetup=True):
    """
    Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    Only include the actual asetup script if asetup=True. This is not needed if the jobPars contain the payload command
    but the pilot still needs to add the exports and the atlasLocalSetup.

    :param asetup: Boolean. True value means that the pilot should include the asetup command.
    :return: asetup (string).
    """

    cmd = ""
    path = "%s/atlas.cern.ch/repo" % get_file_system_root_path()
    if os.path.exists(path):
        cmd = "export ATLAS_LOCAL_ROOT_BASE=%s/ATLASLocalRootBase;" % path
        cmd += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;"
        if asetup:
            cmd += "source $AtlasSetup/scripts/asetup.sh"
    else:
        appdir = get_appdir()
        if appdir == "":
            appdir = os.environ.get('VO_ATLAS_SW_DIR', '')
        if appdir != "":
            if asetup:
                cmd = "source %s/scripts/asetup.sh" % appdir

    return cmd


def get_asetup_options(release, homepackage):
    """
    Determine the proper asetup options.
    :param release: ATLAS release string.
    :param homepackage: ATLAS homePackage string.
    :return: asetup options (string).
    """

    asetupopt = []
    release = re.sub('^Atlas-', '', release)

    # is it a user analysis homePackage?
    if 'AnalysisTransforms' in homepackage:

        _homepackage = re.sub('^AnalysisTransforms-*', '', homepackage)
        if _homepackage == '' or re.search('^\d+\.\d+\.\d+$', release) is None:
            if release != "":
                asetupopt.append(release)
        if _homepackage != '':
            asetupopt += _homepackage.split('_')

    else:

        asetupopt += homepackage.split('/')
        if release not in homepackage:
            asetupopt.append(release)

    # Add the notest,here for all setups (not necessary for late releases but harmless to add)
    asetupopt.append('notest')
    # asetupopt.append('here')

    # Add the fast option if possible (for the moment, check for locally defined env variable)
    if "ATLAS_FAST_ASETUP" in os.environ:
        asetupopt.append('fast')

    return ','.join(asetupopt)


def is_standard_atlas_job(release):
    """
    Is it a standard ATLAS job?
    A job is a standard ATLAS job if the release string begins with 'Atlas-'.

    :param release: Release value (string).
    :return: Boolean. Returns True if standard ATLAS job.
    """

    return release.startswith('Atlas-')
