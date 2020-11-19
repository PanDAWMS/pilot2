#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2020

import os
import re
import glob
from time import sleep

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import NoSoftwareDir
from pilot.info import infosys
from pilot.util.container import execute
from pilot.util.filehandling import read_file, write_file, copy

from .metadata import get_file_info_from_xml

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def get_file_system_root_path():
    """
    Return the root path of the local file system.
    The function returns "/cvmfs" or "/(some path)/cvmfs" in case the expected file system root path is not
    where it usually is (e.g. on an HPC). A site can set the base path by exporting ATLAS_SW_BASE.

    :return: path (string)
    """

    return os.environ.get('ATLAS_SW_BASE', '/cvmfs')


def should_pilot_prepare_setup(noexecstrcnv, jobpars, imagename=None):
    """
    Determine whether the pilot should add the setup to the payload command or not.
    The pilot will not add asetup if jobPars already contain the information (i.e. it was set by the payload creator).
    If noExecStrCnv is set, then jobPars is expected to contain asetup.sh + options
    If a stand-alone container / user defined container is used, pilot should not prepare asetup.

    :param noexecstrcnv: boolean.
    :param jobpars: job parameters (string).
    :param imagename: container image (string).
    :return: boolean.
    """

    if imagename:
        return False

    if noexecstrcnv:
        if "asetup.sh" in jobpars:
            logger.info("asetup will be taken from jobPars")
            preparesetup = False
        else:
            logger.info("noExecStrCnv is set but asetup command was not found in jobPars (pilot will prepare asetup)")
            preparesetup = True
    else:
        logger.info("pilot will prepare the setup")
        preparesetup = True

    return preparesetup


def get_alrb_export(add_if=False):
    """
    Return the export command for the ALRB path if it exists.
    If the path does not exist, return empty string.

    :param add_if: Boolean. True means that an if statement will be placed around the export.
    :return: export command
    """

    path = "%s/atlas.cern.ch/repo" % get_file_system_root_path()
    cmd = "export ATLAS_LOCAL_ROOT_BASE=%s/ATLASLocalRootBase;" % path if os.path.exists(path) else ""

    # if [ -z "$ATLAS_LOCAL_ROOT_BASE" ]; then export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; fi;
    if cmd and add_if:
        cmd = 'if [ -z \"$ATLAS_LOCAL_ROOT_BASE\" ]; then ' + cmd + ' fi;'

    return cmd


def get_asetup(asetup=True, alrb=False, add_if=False):
    """
    Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    Only include the actual asetup script if asetup=True. This is not needed if the jobPars contain the payload command
    but the pilot still needs to add the exports and the atlasLocalSetup.

    :param asetup: Boolean. True value means that the pilot should include the asetup command.
    :param alrb: Boolean. True value means that the function should return special setup used with ALRB and containers.
    :param add_if: Boolean. True means that an if statement will be placed around the export.
    :raises: NoSoftwareDir if appdir does not exist.
    :return: source <path>/asetup.sh (string).
    """

    cmd = ""
    alrb_cmd = get_alrb_export(add_if=add_if)
    if alrb_cmd != "":
        cmd = alrb_cmd
        if not alrb:
            cmd += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;"
            if asetup:
                cmd += "source $AtlasSetup/scripts/asetup.sh"
    else:
        try:  # use try in case infosys has not been initiated
            appdir = infosys.queuedata.appdir
        except Exception:
            appdir = ""
        if appdir == "":
            appdir = os.environ.get('VO_ATLAS_SW_DIR', '')
        if appdir != "":
            # make sure that the appdir exists
            if not os.path.exists(appdir):
                msg = 'appdir does not exist: %s' % appdir
                logger.warning(msg)
                raise NoSoftwareDir(msg)
            if asetup:
                cmd = "source %s/scripts/asetup.sh" % appdir

    # do not return an empty string
    #if not cmd:
    #    cmd = "what?"

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
        if _homepackage == '' or re.search(r'^\d+\.\d+\.\d+$', release) is None:  # Python 3 (added r)
            if release != "":
                asetupopt.append(release)
        if _homepackage != '':
            asetupopt += _homepackage.split('_')

    else:

        asetupopt += homepackage.split('/')
        if release not in homepackage and release not in asetupopt:
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
    _dataset = dataset.split(',')
    for ds in _dataset:
        if "DBRelease" not in ds and ".lib." not in ds:
            inds = ds
            break
    if inds != "":
        logger.info("setting INDS environmental variable to: %s" % (inds))
        os.environ['INDS'] = inds
    else:
        logger.warning("INDS unknown")


def get_analysis_trf(transform, workdir):
    """
    Prepare to download the user analysis transform with curl.
    The function will verify the download location from a known list of hosts.

    :param transform: full trf path (url) (string).
    :param workdir: work directory (string).
    :return: exit code (int), diagnostics (string), transform_name (string)
    """

    ec = 0
    diagnostics = ""

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        search_pattern = "%s/jobO.*.tar.gz" % harvester_workdir
        logger.debug("search_pattern - %s" % search_pattern)
        jobopt_files = glob.glob(search_pattern)
        for jobopt_file in jobopt_files:
            logger.debug("jobopt_file = %s workdir = %s" % (jobopt_file, workdir))
            try:
                copy(jobopt_file, workdir)
            except Exception as e:
                logger.error("could not copy file %s to %s : %s" % (jobopt_file, workdir, e))

    if '/' in transform:
        transform_name = transform.split('/')[-1]
    else:
        logger.warning('did not detect any / in %s (using full transform name)' % transform)
        transform_name = transform

    # is the command already available? (e.g. if already downloaded by a preprocess/main process step)
    if os.path.exists(os.path.join(workdir, transform_name)):
        logger.info('script %s is already available - no need to download again' % transform_name)
        return ec, diagnostics, transform_name

    original_base_url = ""

    # verify the base URL
    for base_url in get_valid_base_urls():
        if transform.startswith(base_url):
            original_base_url = base_url
            break

    if original_base_url == "":
        diagnostics = "invalid base URL: %s" % transform
        return errors.TRFDOWNLOADFAILURE, diagnostics, ""

    # try to download from the required location, if not - switch to backup
    status = False
    for base_url in get_valid_base_urls(order=original_base_url):
        trf = re.sub(original_base_url, base_url, transform)
        logger.debug("attempting to download script: %s" % trf)
        status, diagnostics = download_transform(trf, transform_name, workdir)
        if status:
            break

    if not status:
        return errors.TRFDOWNLOADFAILURE, diagnostics, ""

    logger.info("successfully downloaded script")
    path = os.path.join(workdir, transform_name)
    logger.debug("changing permission of %s to 0o755" % path)
    try:
        os.chmod(path, 0o755)  # Python 2/3
    except Exception as e:
        diagnostics = "failed to chmod %s: %s" % (transform_name, e)
        return errors.CHMODTRF, diagnostics, ""

    return ec, diagnostics, transform_name


def download_transform(url, transform_name, workdir):
    """
    Download the transform from the given url
    :param url: download URL with path to transform (string).
    :param transform_name: trf name (string).
    :param workdir: work directory (string).
    :return:
    """

    status = False
    diagnostics = ""
    path = os.path.join(workdir, transform_name)
    cmd = 'curl -sS \"%s\" > %s' % (url, path)
    trial = 1
    max_trials = 3

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        # skip curl by setting max_trials = 0
        max_trials = 0
        source_path = os.path.join(harvester_workdir, transform_name)
        try:
            copy(source_path, path)
            status = True
        except Exception as error:
            status = False
            diagnostics = "Failed to copy file %s to %s : %s" % (source_path, path, error)
            logger.error(diagnostics)

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
            logger.info("curl command returned: %s" % stdout)
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


def get_payload_environment_variables(cmd, job_id, task_id, attempt_nr, processing_type, site_name, analysis_job):
    """
    Return an array with enviroment variables needed by the payload.

    :param cmd: payload execution command (string).
    :param job_id: PanDA job id (string).
    :param task_id: PanDA task id (string).
    :param attempt_nr: PanDA job attempt number (int).
    :param processing_type: processing type (string).
    :param site_name: site name (string).
    :param analysis_job: True for user analysis jobs, False otherwise (boolean).
    :return: list of environment variables needed by the payload.
    """

    variables = []
    variables.append('export PANDA_RESOURCE=\'%s\';' % site_name)
    variables.append('export FRONTIER_ID=\"[%s_%s]\";' % (task_id, job_id))
    variables.append('export CMSSW_VERSION=$FRONTIER_ID;')
    variables.append('export PandaID=%s;' % os.environ.get('PANDAID', 'unknown'))
    variables.append('export PanDA_TaskID=\'%s\';' % os.environ.get('PanDA_TaskID', 'unknown'))
    variables.append('export PanDA_AttemptNr=\'%d\';' % attempt_nr)
    variables.append('export INDS=\'%s\';' % os.environ.get('INDS', 'unknown'))

    # Unset ATHENA_PROC_NUMBER if set for event service Merge jobs
    if "Merge_tf" in cmd and 'ATHENA_PROC_NUMBER' in os.environ:
        variables.append('unset ATHENA_PROC_NUMBER;')
        variables.append('unset ATHENA_CORE_NUMBER;')

    if analysis_job:
        variables.append('export ROOT_TTREECACHE_SIZE=1;')
        try:
            core_count = int(os.environ.get('ATHENA_PROC_NUMBER'))
        except Exception:
            _core_count = 'export ROOTCORE_NCPUS=1;'
        else:
            _core_count = 'export ROOTCORE_NCPUS=%d;' % core_count
        variables.append(_core_count)

    if processing_type == "":
        logger.warning("RUCIO_APPID needs job.processingType but it is not set!")
    else:
        variables.append('export RUCIO_APPID=\'%s\';' % processing_type)
    variables.append('export RUCIO_ACCOUNT=\'%s\';' % os.environ.get('RUCIO_ACCOUNT', 'pilot'))

    return variables


def get_writetoinput_filenames(writetofile):
    """
    Extract the writeToFile file name(s).
    writeToFile='tmpin_mc16_13TeV.345935.PhPy8EG_A14_ttbarMET100_200_hdamp258p75_nonallhad.merge.AOD.e6620_e5984_s3126_r10724_r10726_tid15760866_00:AOD.15760866._000002.pool.root.1'
    -> return 'tmpin_mc16_13TeV.345935.PhPy8EG_A14_ttbarMET100_200_hdamp258p75_nonallhad.merge.AOD.e6620_e5984_s3126_r10724_r10726_tid15760866_00'

    :param writetofile: string containing file name information.
    :return: list of file names
    """

    filenames = []
    entries = writetofile.split('^')
    for entry in entries:
        if ':' in entry:
            name = entry.split(":")[0]
            name = name.replace('.pool.root.', '.txt.')  # not necessary?
            filenames.append(name)

    return filenames


def replace_lfns_with_turls(cmd, workdir, filename, infiles, writetofile=""):
    """
    Replace all LFNs with full TURLs in the payload execution command.

    This function is used with direct access in production jobs. Athena requires a full TURL instead of LFN.

    :param cmd: payload execution command (string).
    :param workdir: location of metadata file (string).
    :param filename: metadata file name (string).
    :param infiles: list of input files.
    :param writetofile:
    :return: updated cmd (string).
    """

    turl_dictionary = {}  # { LFN: TURL, ..}
    path = os.path.join(workdir, filename)
    if os.path.exists(path):
        file_info_dictionary = get_file_info_from_xml(workdir, filename=filename)
        for inputfile in infiles:
            if inputfile in cmd:
                turl = file_info_dictionary[inputfile][0]
                turl_dictionary[inputfile] = turl
                # if turl.startswith('root://') and turl not in cmd:
                if turl not in cmd:
                    cmd = cmd.replace(inputfile, turl)
                    logger.info("replaced '%s' with '%s' in the run command" % (inputfile, turl))

        # replace the LFNs with TURLs in the writetofile input file list (if it exists)
        if writetofile and turl_dictionary:
            filenames = get_writetoinput_filenames(writetofile)
            logger.info("filenames=%s" % filenames)
            for fname in filenames:
                new_lines = []
                path = os.path.join(workdir, fname)
                if os.path.exists(path):
                    f = read_file(path)
                    for line in f.split('\n'):
                        fname = os.path.basename(line)
                        if fname in turl_dictionary:
                            turl = turl_dictionary[fname]
                            new_lines.append(turl)
                        else:
                            if line:
                                new_lines.append(line)

                    lines = '\n'.join(new_lines)
                    if lines:
                        write_file(path, lines)
                        logger.info("lines=%s" % lines)
                else:
                    logger.warning("file does not exist: %s" % path)
    else:
        logger.warning("could not find file: %s (cannot locate TURLs for direct access)" % filename)

    return cmd
