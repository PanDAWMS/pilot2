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
from time import sleep

from pilot.info import infosys
from pilot.util.container import execute

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


def is_user_analysis_job(trf):  ## DEPRECATED: consider job.is_analysis()
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


def get_alrb_export():
    """
    Return the export command for the ALRB path if it exists.
    If the path does not exist, return empty string.
    :return: export command
    """

    path = "%s/atlas.cern.ch/repo" % get_file_system_root_path()
    if os.path.exists(path):
        cmd = "export ATLAS_LOCAL_ROOT_BASE=%s/ATLASLocalRootBase;" % path
    else:
        cmd = ""
    return cmd


def get_asetup(asetup=True, alrb=False):
    """
    Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    Only include the actual asetup script if asetup=True. This is not needed if the jobPars contain the payload command
    but the pilot still needs to add the exports and the atlasLocalSetup.

    :param asetup: Boolean. True value means that the pilot should include the asetup command.
    :param alrb: Boolean. True value means that the function should return special setup used with ALRB and containers.
    :return: asetup (string).
    """

    cmd = ""
    alrb_cmd = get_alrb_export()
    if alrb_cmd != "":
        cmd = alrb_cmd
        if not alrb:
            cmd += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;"
            if asetup:
                cmd += "source $AtlasSetup/scripts/asetup.sh"
    else:
        appdir = infosys.queuedata.appdir
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


def set_inds(dataset):
    """
    Set the INDS environmental variable used by runAthena.

    :param dataset: dataset for input files (realDatasetsIn) (string).
    :return:
    """

    inds = ""
    for ds in dataset:
        if "DBRelease" not in ds and ".lib." not in ds:
            inds = ds
            break
    if inds != "":
        logger.info("setting INDS environmental variable to: %s" % (inds))
        os.environ['INDS'] = inds
    else:
        logger.warning("INDS unknown")


def get_analysis_trf(transform):
    """
    Prepare to download the user analysis transform with curl.
    The function will verify the download location from a known list of hosts.

    :param transform: full trf path (url) (string).
    :return: exit code (int), diagnostics (string), transform_name (string)
    """

    ec = 0
    diagnostics = ""

    pilot_initdir = os.environ.get('PILOT_HOME', '')
    if '/' in transform:
        transform_name = transform.split('/')[-1]
    else:
        logger.warning('did not detect any / in %s (using full transform name)' % (transform))
        transform_name = transform
    logger.debug("transform_name = %s" % (transform_name))
    original_base_url = ""

    # verify the base URL
    for base_url in get_valid_base_urls():
        if transform.startswith(base_url):
            original_base_url = base_url
            break

    if original_base_url == "":
        pilotErrorDiag = "Invalid base URL: %s" % (transform)
        # return self.__error.ERR_TRFDOWNLOAD, pilotErrorDiag, ""
    else:
        logger.debug("verified the trf base url: %s" % (original_base_url))

    # try to download from the required location, if not - switch to backup
    for base_url in get_valid_base_urls(order=original_base_url):
        trf = re.sub(original_base_url, base_url, transform)
        logger.debug("attempting to download trf: %s" % (trf))
        status, pilotErrorDiag = download_transform(trf, transform_name)
        if status:
            break

    if not status:
        pass
        # return self.__error.ERR_TRFDOWNLOAD, diagnostics, ""

    logger.info("successfully downloaded transform")
    logger.debug("changing permission of %s to 0755" % (transform))
    try:
        os.chmod(transform, 0755)
    except Exception, e:
        diagnostics = "failed to chmod %s: %s" % (transform, e)
        # return self.__error.ERR_CHMODTRF, diagnostics, ""

    return ec, diagnostics, transform_name


def download_transform(url, transform_name):
    """
    Download the transform from the given url
    :param url: download URL with path to transform (string).
    :param transform_name: trf name (string).
    :return:
    """

    status = False
    diagnostics = ""
    cmd = 'curl -sS \"%s\" > %s' % (url, transform_name)
    trial = 1
    max_trials = 3

    # try to download the trf a maximum of 3 times
    while trial <= max_trials:
        logger.info("executing command [trial %d/%d]: %s" % (trial, max_trials, cmd))

        exit_code, stdout, stderr = execute(cmd, mute=True)
        if not stdout:
            stdout = "(None)"
        if exit_code != 0:
            # Analyze exit code / output
            diagnostics = "curl command failed: %d, %s, %s" % (exit_code, stdout, stderr)
            logger.warning(diagnostics)
            if trial == max_trials:
                logger.fatal('could not download transform: %s' % stdout)
                status = False
                break
            else:
                logger.info("will try again after 60 s")
                sleep(60)
        else:
            logger.info("curl command returned: %s" % (stdout))
            status = True
            break
        trial += 1

    return status, diagnostics


def get_valid_base_urls(order=None):
    """
    Return a list of valid base URLs from where the user analysis transform may be downloaded from.
    If order is defined, return given item first.
    E.g. order=http://atlpan.web.cern.ch/atlpan -> ['http://atlpan.web.cern.ch/atlpan', ...]
    NOTE: the URL list may be out of date.

    :param order: order (string).
    :return: valid base URLs (list).
    """

    valid_base_urls = []
    _valid_base_urls = ["http://www.usatlas.bnl.gov",
                        "https://www.usatlas.bnl.gov",
                        "http://pandaserver.cern.ch",
                        "http://atlpan.web.cern.ch/atlpan",
                        "https://atlpan.web.cern.ch/atlpan",
                        "http://classis01.roma1.infn.it",
                        "http://atlas-install.roma1.infn.it"]

    if order:
        valid_base_urls.append(order)
        for url in _valid_base_urls:
            if url != order:
                valid_base_urls.append(url)
    else:
        valid_base_urls = _valid_base_urls

    return valid_base_urls


def tryint(x):
    """
    Used by numbered string comparison (to protect against unexpected letters in version number).

    :param x: possible int.
    :return: converted int or original value in case of ValueError.
    """

    try:
        return int(x)
    except ValueError:
        return x


def split_version(s):
    """
    Split version string into parts and convert the parts into integers when possible.
    Any encountered strings are left as they are.
    The function is used with release strings.
    split_version("1.2.3") = (1,2,3)
    split_version("1.2.Nightly") = (1,2,"Nightly")

    The function can also be used for sorting:
    > names = ['YT4.11', '4.3', 'YT4.2', '4.10', 'PT2.19', 'PT2.9']
    > sorted(names, key=splittedname)
    ['4.3', '4.10', 'PT2.9', 'PT2.19', 'YT4.2', 'YT4.11']

    :param s: release string.
    :return: converted release tuple.
    """

    return tuple(tryint(x) for x in re.split('([^.]+)', s))


def is_greater_or_equal(a, b):
    """
    Is the numbered string a >= b?
    "1.2.3" > "1.2"  -- more digits
    "1.2.3" > "1.2.2"  -- rank based comparison
    "1.3.2" > "1.2.3"  -- rank based comparison
    "1.2.N" > "1.2.2"  -- nightlies checker, always greater

    :param a: numbered string.
    :param b: numbered string.
    :return: boolean.
    """

    return split_version(a) >= split_version(b)
