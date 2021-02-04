#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2020
# - Wen Guan, wen.guan@cern.ch, 2018

import os
import re
import fnmatch
from collections import defaultdict
from glob import glob
from signal import SIGTERM, SIGUSR1

try:
    from functools import reduce  # Python 3
except Exception:
    pass

from .container import create_root_container_command
from .dbrelease import get_dbrelease_version, create_dbrelease
from .setup import should_pilot_prepare_setup, is_standard_atlas_job,\
    set_inds, get_analysis_trf, get_payload_environment_variables, replace_lfns_with_turls
from .utilities import get_memory_monitor_setup, get_network_monitor_setup, post_memory_monitor_action,\
    get_memory_monitor_summary_filename, get_prefetcher_setup, get_benchmark_setup, get_memory_monitor_output_filename,\
    get_metadata_dict_from_txt

from pilot.util.auxiliary import get_resource_name, show_memory_usage
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import TrfDownloadFailure, PilotException
from pilot.util.auxiliary import is_python3
from pilot.util.config import config
from pilot.util.constants import UTILITY_BEFORE_PAYLOAD, UTILITY_WITH_PAYLOAD, UTILITY_AFTER_PAYLOAD_STARTED,\
    UTILITY_AFTER_PAYLOAD, UTILITY_AFTER_PAYLOAD_FINISHED, UTILITY_WITH_STAGEIN, UTILITY_AFTER_PAYLOAD_STARTED2
from pilot.util.container import execute
from pilot.util.filehandling import remove, get_guid, remove_dir_tree, read_list, remove_core_dumps, copy,\
    copy_pilot_source, write_file, read_json, read_file, update_extension, get_local_file_size, calculate_checksum
from pilot.util.tracereport import TraceReport

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def sanity_check():
    """
    Perform an initial sanity check before doing anything else in a given workflow.
    This function can be used to verify importing of modules that are otherwise used much later, but it is better to abort
    the pilot if a problem is discovered early.

    :return: exit code (0 if all is ok, otherwise non-zero exit code).
    """

    exit_code = 0

    #try:
    #    from rucio.client.downloadclient import DownloadClient
    #    from rucio.client.uploadclient import UploadClient
    #    # note: must do something with Download/UploadClients or flake8 will complain - but do not instantiate
    #except Exception as e:
    #    logger.warning('sanity check failed: %s' % e)
    #    exit_code = errors.MIDDLEWAREIMPORTFAILURE

    return exit_code


def validate(job):
    """
    Perform user specific payload/job validation.
    This function will produce a local DBRelease file if necessary (old releases).

    :param job: job object.
    :return: Boolean (True if validation is successful).
    """

    status = True

    if 'DBRelease' in job.jobparams:
        logger.debug('encountered DBRelease info in job parameters - will attempt to create a local DBRelease file')
        version = get_dbrelease_version(job.jobparams)
        if version:
            status = create_dbrelease(version, job.workdir)

    # assign error in case of DBRelease handling failure
    if not status:
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.DBRELEASEFAILURE)

    # make sure that any given images actually exist
    if status:
        if job.imagename and job.imagename.startswith('/'):
            if os.path.exists(job.imagename):
                logger.info('verified that image exists: %s' % job.imagename)
            else:
                status = False
                logger.warning('image does not exist: %s' % job.imagename)
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.IMAGENOTFOUND)

    # cleanup job parameters if only copy-to-scratch
    #if job.only_copy_to_scratch():
    #    logger.debug('job.params=%s' % job.jobparams)
    #    if ' --usePFCTurl' in job.jobparams:
    #        logger.debug('cleaning up --usePFCTurl from job parameters since all input is copy-to-scratch')
    #        job.jobparams = job.jobparams.replace(' --usePFCTurl', '')
    #    if ' --directIn' in job.jobparams:
    #        logger.debug('cleaning up --directIn from job parameters since all input is copy-to-scratch')
    #        job.jobparams = job.jobparams.replace(' --directIn', '')

    return status


def open_remote_files(indata, workdir):
    """
    Verify that direct i/o files can be opened.

    :param indata: list of FileSpec.
    :param workdir: working directory (string).
    :return: exit code (int), diagnostics (string).
    """

    ec = 0
    diagnostics = ""
    not_opened = ""

    # extract direct i/o files from indata (string of comma-separated turls)
    turls = extract_turls(indata)
    if turls:
        # execute file open script which will attempt to open each file

        # copy pilot source into container directory, unless it is already there
        diagnostics = copy_pilot_source(workdir)
        if diagnostics:
            raise PilotException(diagnostics)

        script = 'open_remote_file.py'
        final_script_path = os.path.join(workdir, script)
        os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH') + ':' + workdir
        script_path = os.path.join('pilot/scripts', script)
        d1 = os.path.join(os.path.join(os.environ['PILOT_HOME'], 'pilot2'), script_path)
        d2 = os.path.join(workdir, script_path)
        full_script_path = d1 if os.path.exists(d1) else d2
        if not os.path.exists(full_script_path):
            # do not set ec since this will be a pilot issue rather than site issue
            diagnostics = 'cannot perform file open test - script path does not exist: %s' % full_script_path
            logger.warning(diagnostics)
            logger.warning('tested both path=%s and path=%s (none exists)' % (d1, d2))
            return ec, diagnostics, not_opened
        try:
            copy(full_script_path, final_script_path)
        except Exception as e:
            # do not set ec since this will be a pilot issue rather than site issue
            diagnostics = 'cannot perform file open test - pilot source copy failed: %s' % e
            logger.warning(diagnostics)
            return ec, diagnostics, not_opened
        else:
            # correct the path when containers have been used
            final_script_path = os.path.join('.', script)

            _cmd = get_file_open_command(final_script_path, turls)
            cmd = create_root_container_command(workdir, _cmd)

            show_memory_usage()

            logger.info('*** executing file open verification script:\n\n\'%s\'\n\n' % cmd)
            exit_code, stdout, stderr = execute(cmd, usecontainer=False)
            if config.Pilot.remotefileverification_log:
                write_file(os.path.join(workdir, config.Pilot.remotefileverification_log), stdout + stderr, mute=False)

            show_memory_usage()

            # error handling
            if exit_code:
                logger.warning('script %s finished with ec=%d' % (script, exit_code))
            else:
                dictionary_path = os.path.join(workdir, config.Pilot.remotefileverification_dictionary)
                if not dictionary_path:
                    logger.warning('file does not exist: %s' % dictionary_path)
                else:
                    file_dictionary = read_json(dictionary_path)
                    if not file_dictionary:
                        logger.warning('could not read dictionary from %s' % dictionary_path)
                    else:
                        not_opened = ""
                        for turl in file_dictionary:
                            opened = file_dictionary[turl]
                            logger.info('turl could be opened: %s' % turl) if opened else logger.info('turl could not be opened: %s' % turl)
                            if not opened:
                                not_opened += turl if not not_opened else ",%s" % turl
                        if not_opened:
                            ec = errors.REMOTEFILECOULDNOTBEOPENED
                            diagnostics = "turl not opened:%s" % not_opened if "," not in not_opened else "turls not opened:%s" % not_opened
    else:
        logger.info('nothing to verify (for remote files)')

    return ec, diagnostics, not_opened


def get_file_open_command(script_path, turls):
    """

    :param script_path: path to script (string).
    :return: comma-separated list of turls (string).
    """

    return "%s --turls=%s -w %s" % (script_path, turls, os.path.dirname(script_path))


def extract_turls(indata):
    """
    Extract TURLs from indata for direct i/o files.

    :param indata: list of FileSpec.
    :return: comma-separated list of turls (string).
    """

    turls = ""
    for f in indata:
        if f.status == 'remote_io':
            turls += f.turl if not turls else ",%s" % f.turl

    return turls


def process_remote_file_traces(path, job, not_opened_turls):
    """
    Report traces for remote files.
    The function reads back the base trace report (common part of all traces) and updates it per file before reporting
    it to the Rucio server.

    :param path: path to base trace report (string).
    :param job: job object.
    :param not_opened_turls: list of turls that could not be opened (list).
    :return:
    """

    try:
        base_trace_report = read_json(path)
    except PilotException as e:
        logger.warning('failed to open base trace report (cannot send trace reports): %s' % e)
    else:
        if not base_trace_report:
            logger.warning('failed to read back base trace report (cannot send trace reports)')
        else:
            # update and send the trace info
            for fspec in job.indata:
                if fspec.status == 'remote_io':
                    base_trace_report.update(url=fspec.turl)
                    base_trace_report.update(remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
                    base_trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
                    base_trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
                    if fspec.turl in not_opened_turls:
                        base_trace_report.update(clientState='FAILED_REMOTE_OPEN')
                    else:
                        base_trace_report.update(clientState='FOUND_ROOT')

                    # copy the base trace report (only a dictionary) into a real trace report object
                    trace_report = TraceReport(**base_trace_report)
                    if trace_report:
                        trace_report.send()
                    else:
                        logger.warning('failed to create trace report for turl=%s' % fspec.turl)


def get_payload_command(job):
    """
    Return the full command for executing the payload, including the sourcing of all setup files and setting of
    environment variables.

    :param job: job object.
    :raises PilotException: TrfDownloadFailure.
    :return: command (string).
    """

    show_memory_usage()

    # Should the pilot do the setup or does jobPars already contain the information?
    preparesetup = should_pilot_prepare_setup(job.noexecstrcnv, job.jobparams)

    # Get the platform value
    # platform = job.infosys.queuedata.platform

    # Is it a user job or not?
    userjob = job.is_analysis()
    logger.info('pilot is running a user analysis job') if userjob else logger.info('pilot is running a production job')

    resource_name = get_resource_name()  # 'grid' if no hpc_resource is set
    resource = __import__('pilot.user.atlas.resource.%s' % resource_name, globals(), locals(), [resource_name], 0)  # Python 3, -1 -> 0

    # get the general setup command and then verify it if required
    cmd = resource.get_setup_command(job, preparesetup)
    if cmd:
        ec, diagnostics = resource.verify_setup_command(cmd)
        if ec != 0:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(ec)
            raise PilotException(diagnostics, code=ec)

    # make sure that remote file can be opened before executing payload
    catchall = job.infosys.queuedata.catchall.lower() if job.infosys.queuedata.catchall else ''
    if config.Pilot.remotefileverification_log and 'remoteio_test=false' not in catchall:
        ec = 0
        diagnostics = ""
        not_opened_turls = ""
        try:
            ec, diagnostics, not_opened_turls = open_remote_files(job.indata, job.workdir)
        except Exception as e:
            logger.warning('caught exception: %s' % e)
        else:
            # read back the base trace report
            path = os.path.join(job.workdir, config.Pilot.base_trace_report)
            if not os.path.exists(path):
                logger.warning('base trace report does not exist (%s) - input file traces should already have been sent' % path)
            else:
                process_remote_file_traces(path, job, not_opened_turls)

            # fail the job if the remote files could not be verified
            if ec != 0:
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(ec)
                raise PilotException(diagnostics, code=ec)
    else:
        logger.debug('no remote file open verification')

    if is_standard_atlas_job(job.swrelease):

        # Normal setup (production and user jobs)
        logger.info("preparing normal production/analysis job setup command")
        cmd = get_normal_payload_command(cmd, job, preparesetup, userjob)

    else:  # Generic, non-ATLAS specific jobs, or at least a job with undefined swRelease

        logger.info("generic job (non-ATLAS specific or with undefined swRelease)")
        cmd = get_generic_payload_command(cmd, job, preparesetup, userjob)

    # add any missing trailing ;
    if not cmd.endswith(';'):
        cmd += '; '

    # only if not using a user container
    if not job.imagename:
        site = os.environ.get('PILOT_SITENAME', '')
        variables = get_payload_environment_variables(cmd, job.jobid, job.taskid, job.attemptnr, job.processingtype, site, userjob)
        cmd = ''.join(variables) + cmd

    # prepend PanDA job id in case it is not there already (e.g. runcontainer jobs)
    if 'export PandaID' not in cmd:
        cmd = "export PandaID=%s;" % job.jobid + cmd

    cmd = cmd.replace(';;', ';')

    # For direct access in prod jobs, we need to substitute the input file names with the corresponding TURLs
    # get relevant file transfer info
    #use_copy_tool, use_direct_access, use_pfc_turl = get_file_transfer_info(job)
    #if not userjob and use_direct_access and job.transfertype == 'direct':
    if not userjob and not job.is_build_job() and job.has_remoteio():  ## ported from old logic
        ## ported from old logic but still it looks strange (anisyonk)
        ## the "PoolFileCatalog.xml" should already contains proper TURLs values as it created by create_input_file_metadata()
        ## if the case is just to patch `writetofile` file, than logic should be cleaned and decoupled
        ## anyway, instead of parsing the file, it's much more easy to generate properly `writetofile` content from the beginning with TURL data
        lfns = job.get_lfns_and_guids()[0]
        cmd = replace_lfns_with_turls(cmd, job.workdir, "PoolFileCatalog.xml", lfns, writetofile=job.writetofile)

    # Explicitly add the ATHENA_PROC_NUMBER (or JOB value)
    cmd = add_athena_proc_number(cmd)

    show_memory_usage()

    logger.info('payload run command: %s' % cmd)

    return cmd


def get_normal_payload_command(cmd, job, preparesetup, userjob):
    """
    Return the payload command for a normal production/analysis job.

    :param cmd: any preliminary command setup (string).
    :param job: job object.
    :param userjob: True for user analysis jobs, False otherwise (bool).
    :param preparesetup: True if the pilot should prepare the setup, False if already in the job parameters.
    :return: normal payload command (string).
    """

    # set the INDS env variable (used by runAthena but also for EventIndex production jobs)
    set_inds(job.datasetin)  # realDatasetsIn

    if userjob:
        # Try to download the trf (skip when user container is to be used)
        ec, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir)
        if ec != 0:
            raise TrfDownloadFailure(diagnostics)
        else:
            logger.debug('user analysis trf: %s' % trf_name)

        if preparesetup:
            _cmd = get_analysis_run_command(job, trf_name)
        else:
            _cmd = job.jobparams

        # Correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make)
        cmd += "; " + add_makeflags(job.corecount, "") + _cmd
    else:
        # Add Database commands if they are set by the local site
        cmd += os.environ.get('PILOT_DB_LOCAL_SETUP_CMD', '')

        if job.is_eventservice:
            if job.corecount:
                cmd += '; export ATHENA_PROC_NUMBER=%s' % job.corecount
                cmd += '; export ATHENA_CORE_NUMBER=%s' % job.corecount
            else:
                cmd += '; export ATHENA_PROC_NUMBER=1'
                cmd += '; export ATHENA_CORE_NUMBER=1'

        # Add the transform and the job parameters (production jobs)
        if preparesetup:
            cmd += "; %s %s" % (job.transformation, job.jobparams)
        else:
            cmd += "; " + job.jobparams

    return cmd


def get_generic_payload_command(cmd, job, preparesetup, userjob):
    """

    :param cmd:
    :param job: job object.
    :param preparesetup:
    :param userjob: True for user analysis jobs, False otherwise (bool).
    :return: generic job command (string).
    """

    if userjob:
        # Try to download the trf
        #if job.imagename != "" or "--containerImage" in job.jobparams:
        #    job.transformation = os.path.join(os.path.dirname(job.transformation), "runcontainer")
        #    logger.warning('overwrote job.transformation, now set to: %s' % job.transformation)
        ec, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir)
        if ec != 0:
            raise TrfDownloadFailure(diagnostics)
        else:
            logger.debug('user analysis trf: %s' % trf_name)

        if preparesetup:
            _cmd = get_analysis_run_command(job, trf_name)
        else:
            _cmd = job.jobparams

        # correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make)
        # only if not using a user container
        if not job.imagename:
            cmd += "; " + add_makeflags(job.corecount, "") + _cmd
        else:
            cmd += _cmd

    elif verify_release_string(job.homepackage) != 'NULL' and job.homepackage != ' ':
        if preparesetup:
            cmd = "python %s/%s %s" % (job.homepackage, job.transformation, job.jobparams)
        else:
            cmd = job.jobparams
    else:
        if preparesetup:
            cmd = "python %s %s" % (job.transformation, job.jobparams)
        else:
            cmd = job.jobparams

    return cmd


def add_athena_proc_number(cmd):
    """
    Add the ATHENA_PROC_NUMBER and ATHENA_CORE_NUMBER to the payload command if necessary.

    :param cmd: payload execution command (string).
    :return: updated payload execution command (string).
    """

    # get the values if they exist
    try:
        value1 = int(os.environ['ATHENA_PROC_NUMBER_JOB'])
    except Exception as e:
        logger.warning('failed to convert ATHENA_PROC_NUMBER_JOB to int: %s' % e)
        value1 = None
    try:
        value2 = int(os.environ['ATHENA_CORE_NUMBER'])
    except Exception as e:
        logger.warning('failed to convert ATHENA_CORE_NUMBER to int: %s' % e)
        value2 = None

    if "ATHENA_PROC_NUMBER" not in cmd:
        if "ATHENA_PROC_NUMBER" in os.environ:
            cmd = 'export ATHENA_PROC_NUMBER=%s;' % os.environ['ATHENA_PROC_NUMBER'] + cmd
        elif "ATHENA_PROC_NUMBER_JOB" in os.environ and value1:
            if value1 > 1:
                cmd = 'export ATHENA_PROC_NUMBER=%d;' % value1 + cmd
            else:
                logger.info("will not add ATHENA_PROC_NUMBER to cmd since the value is %s" % str(value1))
        else:
            logger.warning("don't know how to set ATHENA_PROC_NUMBER (could not find it in os.environ)")
    else:
        logger.info("ATHENA_PROC_NUMBER already in job command")

    if 'ATHENA_CORE_NUMBER' in os.environ and value2:
        if value2 > 1:
            cmd = 'export ATHENA_CORE_NUMBER=%d;' % value2 + cmd
        else:
            logger.info("will not add ATHENA_CORE_NUMBER to cmd since the value is %s" % str(value2))
    else:
        logger.warning('there is no ATHENA_CORE_NUMBER in os.environ (cannot add it to payload command)')

    return cmd


def verify_release_string(release):
    """
    Verify that the release (or homepackage) string is set.

    :param release: release or homepackage string that might or might not be set.
    :return: release (set string).
    """

    if release is None:
        release = ""
    release = release.upper()
    if release == "":
        release = "NULL"
    if release == "NULL":
        logger.info("detected unset (NULL) release/homepackage string")

    return release


def add_makeflags(job_core_count, cmd):
    """
    Correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make).

    :param job_core_count: core count from the job definition (int).
    :param cmd: payload execution command (string).
    :return: updated payload execution command (string).
    """

    # ATHENA_PROC_NUMBER is set in Node.py using the schedconfig value
    try:
        core_count = int(os.environ.get('ATHENA_PROC_NUMBER'))
    except Exception:
        core_count = -1
    if core_count == -1:
        try:
            core_count = int(job_core_count)
        except Exception:
            pass
        else:
            if core_count >= 1:
                # Note: the original request (AF) was to use j%d and not -j%d, now using the latter
                cmd += "export MAKEFLAGS=\'-j%d QUICK=1 -l1\';" % (core_count)

    # make sure that MAKEFLAGS is always set
    if "MAKEFLAGS=" not in cmd:
        cmd += "export MAKEFLAGS=\'-j1 QUICK=1 -l1\';"

    return cmd


def get_analysis_run_command(job, trf_name):
    """
    Return the proper run command for the user job.

    Example output: export X509_USER_PROXY=<..>;./runAthena <job parameters> --usePFCTurl --directIn

    :param job: job object.
    :param trf_name: name of the transform that will run the job (string). Used when containers are not used.
    :return: command (string).
    """

    cmd = ""

    # get relevant file transfer info
    #use_copy_tool, use_direct_access, use_pfc_turl = get_file_transfer_info(job)
    # check if the input files are to be accessed locally (ie if prodDBlockToken is set to local)
    #if job.is_local():   ## useless since stage-in phase has already passed (DEPRECATE ME, anisyonk)
    #    logger.debug('switched off direct access for local prodDBlockToken')
    #    use_direct_access = False
    #    use_pfc_turl = False

    # add the user proxy
    if 'X509_USER_PROXY' in os.environ and not job.imagename:
        cmd += 'export X509_USER_PROXY=%s;' % os.environ.get('X509_USER_PROXY')

    # set up trfs
    if job.imagename == "":  # user jobs with no imagename defined
        cmd += './%s %s' % (trf_name, job.jobparams)
    else:
        if job.is_analysis() and job.imagename:
            cmd += './%s %s' % (trf_name, job.jobparams)
        else:
            cmd += 'python %s %s' % (trf_name, job.jobparams)

        imagename = job.imagename
        # check if image is on disk as defined by envar PAYLOAD_CONTAINER_LOCATION
        payload_container_location = os.environ.get('PAYLOAD_CONTAINER_LOCATION')
        if payload_container_location is not None:
            logger.debug("$PAYLOAD_CONTAINER_LOCATION = %s" % payload_container_location)
            # get container name
            containername = imagename.rsplit('/')[-1]
            image_location = os.path.join(payload_container_location, containername)
            if os.path.exists(image_location):
                logger.debug("image exists at %s" % image_location)
                imagename = image_location

        # restore the image name if necessary
        if 'containerImage' not in cmd and 'runcontainer' in trf_name:
            cmd += ' --containerImage=%s' % imagename

    # add control options for PFC turl and direct access
    #if job.indata:   ## DEPRECATE ME (anisyonk)
    #    if use_pfc_turl and '--usePFCTurl' not in cmd:
    #        cmd += ' --usePFCTurl'
    #    if use_direct_access and '--directIn' not in cmd:
    #        cmd += ' --directIn'

    if job.has_remoteio():
        logger.debug('direct access (remoteio) is used to access some input files: --usePFCTurl and --directIn will be added to payload command')
        if '--usePFCTurl' not in cmd:
            cmd += ' --usePFCTurl'
        if '--directIn' not in cmd:
            cmd += ' --directIn'

    # update the payload command for forced accessmode
    ## -- REDUNDANT logic, since it should be done from the beginning at the step of FileSpec initialization (anisyonk)
    #cmd = update_forced_accessmode(log, cmd, job.transfertype, job.jobparams, trf_name)  ## DEPRECATE ME (anisyonk)

    # add guids when needed
    # get the correct guids list (with only the direct access files)
    if not job.is_build_job():
        lfns, guids = job.get_lfns_and_guids()
        _guids = get_guids_from_jobparams(job.jobparams, lfns, guids)
        if _guids:
            cmd += ' --inputGUIDs \"%s\"' % (str(_guids))

    show_memory_usage()

    return cmd


## SHOULD NOT BE USED since payload cmd should be properly generated from the beginning (consider final directio settings) (anisyonk)
def update_forced_accessmode(log, cmd, transfertype, jobparams, trf_name):  ## DEPRECATE ME (anisyonk)
    """
    Update the payload command for forced accessmode.
    accessmode is an option that comes from HammerCloud and is used to force a certain input file access mode; i.e.
    copy-to-scratch or direct access.

    :param log: logging object.
    :param cmd: payload command.
    :param transfertype: transfer type (.e.g 'direct') from the job definition with priority over accessmode (string).
    :param jobparams: job parameters (string).
    :param trf_name: transformation name (string).
    :return: updated payload command string.
    """

    if "accessmode" in cmd and transfertype != 'direct':
        accessmode_usect = None
        accessmode_directin = None
        _accessmode_dic = {"--accessmode=copy": ["copy-to-scratch mode", ""],
                           "--accessmode=direct": ["direct access mode", " --directIn"]}

        # update run_command according to jobPars
        for _mode in list(_accessmode_dic.keys()):  # Python 2/3
            if _mode in jobparams:
                # any accessmode set in jobPars should overrule schedconfig
                logger.info("enforcing %s" % _accessmode_dic[_mode][0])
                if _mode == "--accessmode=copy":
                    # make sure direct access is turned off
                    accessmode_usect = True
                    accessmode_directin = False
                elif _mode == "--accessmode=direct":
                    # make sure copy-to-scratch gets turned off
                    accessmode_usect = False
                    accessmode_directin = True
                else:
                    accessmode_usect = False
                    accessmode_directin = False

                # update run_command (do not send the accessmode switch to runAthena)
                cmd += _accessmode_dic[_mode][1]
                if _mode in cmd:
                    cmd = cmd.replace(_mode, "")

        # force usage of copy tool for stage-in or direct access
        if accessmode_usect:
            logger.info('forced copy tool usage selected')
            # remove again the "--directIn"
            if "directIn" in cmd:
                cmd = cmd.replace(' --directIn', ' ')
        elif accessmode_directin:
            logger.info('forced direct access usage selected')
            if "directIn" not in cmd:
                cmd += ' --directIn'
        else:
            logger.warning('neither forced copy tool usage nor direct access was selected')

        if "directIn" in cmd and "usePFCTurl" not in cmd:
            cmd += ' --usePFCTurl'

        # need to add proxy if not there already
        if "--directIn" in cmd and "export X509_USER_PROXY" not in cmd:
            if 'X509_USER_PROXY' in os.environ:
                cmd = cmd.replace("./%s" % trf_name, "export X509_USER_PROXY=%s;./%s" %
                                  (os.environ.get('X509_USER_PROXY'), trf_name))

    # if both direct access and the accessmode loop added a directIn switch, remove the first one from the string
    if cmd.count("directIn") > 1:
        cmd = cmd.replace(' --directIn', ' ', 1)

    return cmd


def get_guids_from_jobparams(jobparams, infiles, infilesguids):
    """
    Extract the correct guid from the input file list.
    The guids list is used for direct reading.
    1. extract input file list for direct reading from job parameters
    2. for each input file in this list, find the corresponding guid from the input file guid list
    Since the job parameters string is entered by a human, the order of the input files might not be the same.

    :param jobparams: job parameters.
    :param infiles: input file list.
    :param infilesguids: input file guids list.
    :return: guids list.
    """

    guidlist = []
    jobparams = jobparams.replace("'", "")
    jobparams = jobparams.replace(", ", ",")

    pattern = re.compile(r'\-i \"\[([A-Za-z0-9.,_-]+)\]\"')
    directreadinginputfiles = re.findall(pattern, jobparams)
    _infiles = []
    if directreadinginputfiles != []:
        _infiles = directreadinginputfiles[0].split(",")
    else:
        match = re.search(r"-i ([A-Za-z0-9.\[\],_-]+) ", jobparams)  # Python 3 (added r)
        if match is not None:
            compactinfiles = match.group(1)
            match = re.search(r'(.*)\[(.+)\](.*)\[(.+)\]', compactinfiles)  # Python 3 (added r)
            if match is not None:
                infiles = []
                head = match.group(1)
                tail = match.group(3)
                body = match.group(2).split(',')
                attr = match.group(4).split(',')
                for idx in range(len(body)):
                    lfn = '%s%s%s%s' % (head, body[idx], tail, attr[idx])
                    infiles.append(lfn)
            else:
                infiles = [compactinfiles]

    if _infiles != []:
        for infile in _infiles:
            # get the corresponding index from the inputFiles list, which has the same order as infilesguids
            try:
                index = infiles.index(infile)
            except Exception as e:
                logger.warning("exception caught: %s (direct reading will fail)" % e)
            else:
                # add the corresponding guid to the list
                guidlist.append(infilesguids[index])

    return guidlist


def get_file_transfer_info(job):   ## TO BE DEPRECATED, NOT USED (anisyonk)
    """
    Return information about desired file transfer.

    :param job: job object
    :return: use copy tool (boolean), use direct access (boolean), use PFC Turl (boolean).
    """

    use_copy_tool = True
    use_direct_access = False
    use_pfc_turl = False

    # check with schedconfig
    if (job.infosys.queuedata.direct_access_lan or job.infosys.queuedata.direct_access_wan or job.transfertype == 'direct') and not job.is_build_job():
        # override if all input files are copy-to-scratch
        if job.only_copy_to_scratch():
            logger.info('all input files are copy-to-scratch (--usePFCTurl and --directIn will not be set)')
        else:
            logger.debug('--usePFCTurl and --directIn will be set')
            use_copy_tool = False
            use_direct_access = True
            use_pfc_turl = True

    return use_copy_tool, use_direct_access, use_pfc_turl


def update_job_data(job):
    """
    This function can be used to update/add data to the job object.
    E.g. user specific information can be extracted from other job object fields. In the case of ATLAS, information
    is extracted from the metadata field and added to other job object fields.

    :param job: job object
    :return:
    """

    ## comment from Alexey:
    ## it would be better to reallocate this logic (as well as parse metadata values)directly to Job object
    ## since in general it's Job related part
    ## later on once we introduce VO specific Job class (inherited from JobData) this can be easily customized

    # get label "all" or "log"
    stageout = get_stageout_label(job)

    if 'exeErrorDiag' in job.metadata:
        job.exeerrordiag = job.metadata['exeErrorDiag']
        if job.exeerrordiag:
            logger.warning('payload failed: exeErrorDiag=%s' % job.exeerrordiag)

    # determine what should be staged out
    job.stageout = stageout  # output and log file or only log file

    work_attributes = None
    try:
        work_attributes = parse_jobreport_data(job.metadata)
    except Exception as e:
        logger.warning('failed to parse job report (cannot set job.nevents): %s' % e)
    else:
        # note: the number of events can be set already at this point if the value was extracted from the job report
        # (a more thorough search for this value is done later unless it was set here)
        nevents = work_attributes.get('nEvents', 0)
        if nevents:
            job.nevents = nevents

    # some HPO jobs will produce new output files (following lfn name pattern), discover those and replace the job.outdata list
    if job.is_hpo:
        update_output_for_hpo(job)

    # extract output files from the job report if required, in case the trf has created additional (overflow) files
    # also make sure all guids are assigned (use job report value if present, otherwise generate the guid)
    if job.metadata and not job.is_eventservice:
        extract_output_file_guids(job)  # keep this for now, complicated to merge with verify_output_files?
        try:
            verify_output_files(job)
        except Exception as e:
            logger.warning('exception caught while trying verify output files: %s' % e)
    else:
        if not job.allownooutput:  # i.e. if it's an empty list/string, do nothing
            logger.debug("will not try to extract output files from jobReport for user job (and allowNoOut list is empty)")
        else:
            # remove the files listed in allowNoOutput if they don't exist
            remove_no_output_files(job)

    ## validate output data (to be moved into the JobData)
    ## warning: do no execute this code unless guid lookup in job report has failed - pilot should only generate guids
    ## if they are not present in job report
    for dat in job.outdata:
        if not dat.guid:
            dat.guid = get_guid()
            logger.warning('guid not set: generated guid=%s for lfn=%s' % (dat.guid, dat.lfn))


def get_stageout_label(job):
    """
    Get a proper stage-out label.

    :param job: job object.
    :return: "all"/"log" depending on stage-out type (string).
    """

    stageout = "all"

    if job.is_eventservice:
        logger.info('event service payload, will only stage-out log')
        stageout = "log"
    else:
        # handle any error codes
        if 'exeErrorCode' in job.metadata:
            job.exeerrorcode = job.metadata['exeErrorCode']
            if job.exeerrorcode == 0:
                stageout = "all"
            else:
                logger.info('payload failed: exeErrorCode=%d' % job.exeerrorcode)
                stageout = "log"

    return stageout


def update_output_for_hpo(job):
    """
    Update the output (outdata) for HPO jobs.

    :param job: job object.
    :return:
    """

    try:
        new_outdata = discover_new_outdata(job)
    except Exception as e:
        logger.warning('exception caught while discovering new outdata: %s' % e)
    else:
        if new_outdata:
            logger.info('replacing job outdata with discovered output (%d file(s))' % len(new_outdata))
            job.outdata = new_outdata


def discover_new_outdata(job):
    """
    Discover new outdata created by HPO job.

    :param job: job object.
    :return: new_outdata (list of FileSpec objects)
    """

    from pilot.info.filespec import FileSpec
    new_outdata = []

    for outdata_file in job.outdata:
        new_output = discover_new_output(outdata_file.lfn, job.workdir)
        if new_output:
            # create new FileSpec objects out of the new output
            for outfile in new_output:
                # note: guid will be taken from job report after this function has been called
                files = [{'scope': outdata_file.scope, 'lfn': outfile, 'workdir': job.workdir,
                          'dataset': outdata_file.dataset, 'ddmendpoint': outdata_file.ddmendpoint,
                          'ddmendpoint_alt': None, 'filesize': new_output[outfile]['filesize'],
                          'checksum': new_output[outfile]['checksum'], 'guid': ''}]
                # do not abbreviate the following two lines as otherwise the content of xfiles will be a list of generator objects
                _xfiles = [FileSpec(type='output', **f) for f in files]
                new_outdata += _xfiles

    return new_outdata


def discover_new_output(name_pattern, workdir):
    """
    Discover new output created by HPO job in the given work dir.

    name_pattern for known 'filename' is 'filename_N' (N = 0, 1, 2, ..).
    Example: name_pattern = 23578835.metrics.000001.tgz
             should discover files with names 23578835.metrics.000001.tgz_N (N = 0, 1, ..)

    new_output = { lfn: {'path': path, 'size': size, 'checksum': checksum}, .. }

    :param name_pattern: assumed name pattern for file to discover (string).
    :param workdir: work directory (string).
    :return: new_output (dictionary).
    """

    new_output = {}
    outputs = glob("%s/%s_*" % (workdir, name_pattern))
    if outputs:
        lfns = [os.path.basename(path) for path in outputs]
        for lfn, path in list(zip(lfns, outputs)):
            # get file size
            filesize = get_local_file_size(path)
            # get checksum
            checksum = calculate_checksum(path)

            if filesize and checksum:
                new_output[lfn] = {'path': path, 'filesize': filesize, 'checksum': checksum}
            else:
                logger.warning('failed to create file info (filesize=%d, checksum=%s) for lfn=%s' %
                               (filesize, checksum, lfn))
    return new_output


def extract_output_file_guids(job):
    """
    Extract output file info from the job report and make sure all guids are assigned (use job report value if present,
    otherwise generate the guid - note: guid generation is done later, not in this function since this function
    might not be called if metadata info is not found prior to the call).

    :param job: job object.
    :return:
    """

    # make sure there is a defined output file list in the job report - unless it is allowed by task parameter allowNoOutput
    if not job.allownooutput:
        output = job.metadata.get('files', {}).get('output', [])
        if output:
            logger.info('verified that job report contains metadata for %d file(s)' % len(output))
        else:
            logger.warning('job report contains no output files and allowNoOutput is not set')  #- will fail job since allowNoOutput is not set')
            #job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOOUTPUTINJOBREPORT)
            return

    # extract info from metadata (job report JSON)
    data = dict([e.lfn, e] for e in job.outdata)
    #extra = []
    for dat in job.metadata.get('files', {}).get('output', []):
        for fdat in dat.get('subFiles', []):
            lfn = fdat['name']

            # verify the guid if the lfn is known
            # only extra guid if the file is known by the job definition (March 18 change, v 2.5.2)
            if lfn in data:
                data[lfn].guid = fdat['file_guid']
                logger.info('set guid=%s for lfn=%s (value taken from job report)' % (data[lfn].guid, lfn))
            else:  # found new entry
                logger.warning('pilot no longer considers output files not mentioned in job definition (lfn=%s)' % lfn)
                continue

                #if job.outdata:
                #    kw = {'lfn': lfn,
                #          'scope': job.outdata[0].scope,  ## take value from 1st output file?
                #          'guid': fdat['file_guid'],
                #          'filesize': fdat['file_size'],
                #          'dataset': dat.get('dataset') or job.outdata[0].dataset  ## take value from 1st output file?
                #          }
                #    spec = FileSpec(filetype='output', **kw)
                #    extra.append(spec)

    # make sure the output list has set guids from job report
    for fspec in job.outdata:
        if fspec.guid != data[fspec.lfn].guid:
            fspec.guid = data[fspec.lfn].guid
            logger.debug('reset guid=%s for lfn=%s' % (fspec.guid, fspec.lfn))
        else:
            if fspec.guid:
                logger.debug('verified guid=%s for lfn=%s' % (fspec.guid, fspec.lfn))
            else:
                logger.warning('guid not set for lfn=%s' % fspec.lfn)
    #if extra:
        #logger.info('found extra output files in job report, will overwrite output file list: extra=%s' % extra)
        #job.outdata = extra


def verify_output_files(job):
    """
    Make sure that the known output files from the job definition are listed in the job report and number of processed events
    is greater than zero. If the output file is not listed in the job report, then if the file is listed in allowNoOutput
    remove it from stage-out, otherwise fail the job.

    Note from Rod: fail scenario: The output file is not in output:[] or is there with zero events. Then if allownooutput is not
    set - fail the job. If it is set, then do not store the output, and finish ok.

    :param job: job object.
    :return: Boolean (and potentially updated job.outdata list)
    """

    failed = False

    # get list of output files from the job definition
    lfns_jobdef = []
    for fspec in job.outdata:
        lfns_jobdef.append(fspec.lfn)
    if not lfns_jobdef:
        logger.debug('empty output file list from job definition (nothing to verify)')
        return True

    # get list of output files from job report
    # (if None is returned, it means the job report is from an old release and does not contain an output list)
    output = job.metadata.get('files', {}).get('output', None)
    if not output and output is not None:
        # ie empty list, output=[] - are all known output files in allowNoOutput?
        logger.warning('encountered an empty output file list in job report, consulting allowNoOutput list')
        failed = False
        for lfn in lfns_jobdef:
            if lfn not in job.allownooutput:
                if job.is_analysis():
                    logger.warning('lfn %s is not in allowNoOutput list - ignore for user job' % lfn)
                else:
                    failed = True
                    logger.warning('lfn %s is not in allowNoOutput list - job will fail' % lfn)
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.MISSINGOUTPUTFILE)
                    break
            else:
                logger.info('lfn %s listed in allowNoOutput - will be removed from stage-out' % lfn)
                remove_from_stageout(lfn, job)

    elif output is None:
        # ie job report is ancient / output could not be extracted
        logger.warning('output file list could not be extracted from job report (nothing to verify)')
    else:
        verified = verify_extracted_output_files(output, lfns_jobdef, job)
        failed = True if not verified else False

    status = True if not failed else False

    if status:
        logger.info('output file verification succeeded')
    else:
        logger.warning('output file verification failed')

    return status


def verify_extracted_output_files(output, lfns_jobdef, job):
    """
    Make sure all output files extracted from the job report are listed.

    :param output: list of FileSpecs (list).
    :param lfns_jobdef: list of lfns strings from job definition (list).
    :param job: job object.
    :return: True if successful, False if failed (Boolean)
    """

    failed = False

    output_jobrep = {}  # {lfn: nentries, ..}
    logger.debug('extracted output file list from job report - make sure all known output files are listed')

    # first collect the output files from the job report
    for dat in output:
        for fdat in dat.get('subFiles', []):
            # get the lfn
            name = fdat.get('name', None)

            # get the number of processed events and add the output file info to the dictionary
            output_jobrep[name] = fdat.get('nentries', None)

    # now make sure that the known output files are in the job report dictionary
    for lfn in lfns_jobdef:
        if lfn not in output_jobrep and lfn not in job.allownooutput:
            if job.is_analysis():
                logger.warning(
                    'output file %s from job definition is not present in job report and is not listed in allowNoOutput' % lfn)
            else:
                logger.warning(
                    'output file %s from job definition is not present in job report and is not listed in allowNoOutput - job will fail' % lfn)
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.MISSINGOUTPUTFILE)
                failed = True
                break
        if lfn not in output_jobrep and lfn in job.allownooutput:
            logger.warning(
                'output file %s from job definition is not present in job report but is listed in allowNoOutput - remove from stage-out' % lfn)
            remove_from_stageout(lfn, job)
        else:
            nentries = output_jobrep[lfn]
            if nentries == "UNDEFINED":
                logger.warning('encountered file with nentries=UNDEFINED - will ignore %s' % lfn)
                continue
            elif nentries is None and lfn not in job.allownooutput:
                logger.warning(
                    'output file %s is listed in job report, but has no events and is not listed in allowNoOutput - will ignore' % lfn)
                continue
            elif nentries is None and lfn in job.allownooutput:
                logger.warning(
                    'output file %s is listed in job report, nentries is None and is listed in allowNoOutput - remove from stage-out' % lfn)
                remove_from_stageout(lfn, job)
            elif type(nentries) is int and nentries == 0 and lfn not in job.allownooutput:
                logger.warning(
                    'output file %s is listed in job report, has zero events and is not listed in allowNoOutput - will ignore' % lfn)
            elif type(nentries) is int and nentries == 0 and lfn in job.allownooutput:
                logger.warning(
                    'output file %s is listed in job report, has zero events and is listed in allowNoOutput - remove from stage-out' % lfn)
                remove_from_stageout(lfn, job)
            elif type(nentries) is int and nentries:
                logger.info('output file %s has %d event(s)' % (lfn, nentries))
            else:  # should not reach this step
                logger.warning('case not handled for output file %s with %s event(s) (ignore)' % (lfn, str(nentries)))

    return False if failed else True


def remove_from_stageout(lfn, job):
    """
    From the given lfn from the stage-out list.

    :param lfn: local file name (string).
    :param job: job object
    :return: [updated job object]
    """

    outdata = []
    for fspec in job.outdata:
        if fspec.lfn == lfn:
            logger.info('removing %s from stage-out list' % lfn)
        else:
            outdata.append(fspec)
    job.outdata = outdata


def remove_no_output_files(job):
    """
    Remove files from output file list if they are listed in allowNoOutput and do not exist.

    :param job: job object.
    :return:
    """

    # first identify the files to keep
    _outfiles = []
    for fspec in job.outdata:
        filename = fspec.lfn
        path = os.path.join(job.workdir, filename)

        if filename in job.allownooutput:
            if os.path.exists(path):
                logger.info("file %s is listed in allowNoOutput but exists (will not be removed from list of files to be staged-out)" % filename)
                _outfiles.append(filename)
            else:
                logger.info("file %s is listed in allowNoOutput and does not exist (will be removed from list of files to be staged-out)" % filename)
        else:
            if os.path.exists(path):
                logger.info("file %s is not listed in allowNoOutput (will be staged-out)" % filename)
            else:
                logger.warning("file %s is not listed in allowNoOutput and does not exist (job will fail)" % filename)
            _outfiles.append(filename)

    # now remove the unwanted fspecs
    if len(_outfiles) != len(job.outdata):
        outdata = []
        for fspec in job.outdata:
            if fspec.lfn in _outfiles:
                outdata.append(fspec)
        job.outdata = outdata


def get_outfiles_records(subfiles):
    """
    Extract file info from job report JSON subfiles entry.

    :param subfiles: list of subfiles.
    :return: file info dictionary with format { 'guid': .., 'size': .., 'nentries': .. (optional)}
    """

    res = {}
    for f in subfiles:
        res[f['name']] = {'guid': f['file_guid'],
                          'size': f['file_size']}
        nentries = f.get('nentries', 'UNDEFINED')
        if type(nentries) == int:
            res[f['name']]['nentries'] = nentries
        else:
            logger.warning("nentries is undefined in job report")

    return res


class DictQuery(dict):
    def get(self, path, dst_dict, dst_key):
        keys = path.split("/")
        if len(keys) == 0:
            return
        last_key = keys.pop()
        v = self
        for key in keys:
            if key in v and isinstance(v[key], dict):
                v = v[key]
            else:
                return
        if last_key in v:
            dst_dict[dst_key] = v[last_key]


def parse_jobreport_data(job_report):
    """
    Parse a job report and extract relevant fields.

    :param job_report:
    :return:
    """
    work_attributes = {}
    if job_report is None or not any(job_report):
        return work_attributes

    # these are default values for job metrics
    core_count = ""
    work_attributes["nEvents"] = 0
    work_attributes["dbTime"] = ""
    work_attributes["dbData"] = ""
    work_attributes["inputfiles"] = []
    work_attributes["outputfiles"] = []

    if "ATHENA_PROC_NUMBER" in os.environ:
        logger.debug("ATHENA_PROC_NUMBER: {0}".format(os.environ["ATHENA_PROC_NUMBER"]))
        work_attributes['core_count'] = int(os.environ["ATHENA_PROC_NUMBER"])
        core_count = int(os.environ["ATHENA_PROC_NUMBER"])

    dq = DictQuery(job_report)
    dq.get("resource/transform/processedEvents", work_attributes, "nEvents")
    dq.get("resource/transform/cpuTimeTotal", work_attributes, "cpuConsumptionTime")
    dq.get("resource/machine/node", work_attributes, "node")
    dq.get("resource/machine/model_name", work_attributes, "cpuConsumptionUnit")
    dq.get("resource/dbTimeTotal", work_attributes, "dbTime")
    dq.get("resource/dbDataTotal", work_attributes, "dbData")
    dq.get("exitCode", work_attributes, "transExitCode")
    dq.get("exitMsg", work_attributes, "exeErrorDiag")
    dq.get("files/input", work_attributes, "inputfiles")
    dq.get("files/output", work_attributes, "outputfiles")

    outputfiles_dict = {}
    for of in work_attributes['outputfiles']:
        outputfiles_dict.update(get_outfiles_records(of['subFiles']))
    work_attributes['outputfiles'] = outputfiles_dict

    if work_attributes['inputfiles']:
        if is_python3():
            work_attributes['nInputFiles'] = reduce(lambda a, b: a + b, [len(inpfiles['subFiles']) for inpfiles in
                                                                         work_attributes['inputfiles']])
        else:
            work_attributes['nInputFiles'] = reduce(lambda a, b: a + b, map(lambda inpfiles: len(inpfiles['subFiles']),
                                                                            work_attributes['inputfiles']))

    if 'resource' in job_report and 'executor' in job_report['resource']:
        j = job_report['resource']['executor']
        exc_report = []
        fin_report = defaultdict(int)
        try:
            _tmplist = filter(lambda d: 'memory' in d and ('Max' or 'Avg' in d['memory']), j.itervalues())  # Python 2
        except Exception:
            _tmplist = [d for d in iter(list(j.values())) if
                        'memory' in d and ('Max' or 'Avg' in d['memory'])]  # Python 3
        for v in _tmplist:
            if 'Avg' in v['memory']:
                exc_report.extend(list(v['memory']['Avg'].items()))  # Python 2/3
            if 'Max' in v['memory']:
                exc_report.extend(list(v['memory']['Max'].items()))  # Python 2/3
        for x in exc_report:
            fin_report[x[0]] += x[1]
        work_attributes.update(fin_report)

    workdir_size = get_workdir_size()
    work_attributes['jobMetrics'] = 'coreCount=%s nEvents=%s dbTime=%s dbData=%s workDirSize=%s' % \
                                    (core_count,
                                        work_attributes["nEvents"],
                                        work_attributes["dbTime"],
                                        work_attributes["dbData"],
                                        workdir_size)
    del(work_attributes["dbData"])
    del(work_attributes["dbTime"])

    return work_attributes


def get_workdir_size():
    """
    Tmp function - move later to file_handling

    :return:
    """
    c, o, e = execute('du -s', shell=True)
    if o is not None:
        return o.split()[0]
    return None


def get_executor_dictionary(jobreport_dictionary):
    """
    Extract the 'executor' dictionary from with a job report.

    :param jobreport_dictionary:
    :return: executor_dictionary
    """

    executor_dictionary = {}
    if jobreport_dictionary != {}:

        if 'resource' in jobreport_dictionary:
            resource_dictionary = jobreport_dictionary['resource']
            if 'executor' in resource_dictionary:
                executor_dictionary = resource_dictionary['executor']
            else:
                logger.warning("no such key: executor")
        else:
            logger.warning("no such key: resource")

    return executor_dictionary


def get_number_of_events_deprecated(jobreport_dictionary):  # TODO: remove this function
    """
    Extract the number of events from the job report.

    :param jobreport_dictionary:
    :return:
    """

    nevents = {}  # FORMAT: { format : total_events, .. }
    nmax = 0

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for format in list(executor_dictionary.keys()):  # "RAWtoESD", .., Python 2/3
            if 'nevents' in executor_dictionary[format]:
                if format in nevents:
                    nevents[format] += executor_dictionary[format]['nevents']
                else:
                    nevents[format] = executor_dictionary[format]['nevents']
            else:
                logger.warning("format %s has no such key: nevents" % (format))

    # Now find the largest number of events among the different formats
    if nevents != {}:
        try:
            nmax = max(nevents.values())
        except Exception as e:
            logger.warning("exception caught: %s" % (e))
            nmax = 0
    else:
        logger.warning("did not find the number of events in the job report")
        nmax = 0

    return nmax


def get_db_info(jobreport_dictionary):
    """
    Extract and add up the DB info from the job report.
    This information is reported with the jobMetrics.
    Note: this function adds up the different dbData and dbTime's in the different executor steps. In modern job
    reports this might have been done already by the transform and stored in dbDataTotal and dbTimeTotal.

    :param jobreport_dictionary: job report dictionary.
    :return: db_time (int), db_data (long)
    """

    db_time = 0
    try:
        db_data = long(0)  # Python 2
    except Exception:
        db_data = 0  # Python 3

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for format in list(executor_dictionary.keys()):  # "RAWtoESD", .., Python 2/3
            if 'dbData' in executor_dictionary[format]:
                try:
                    db_data += executor_dictionary[format]['dbData']
                except Exception:
                    pass
            else:
                logger.warning("format %s has no such key: dbData" % (format))
            if 'dbTime' in executor_dictionary[format]:
                try:
                    db_time += executor_dictionary[format]['dbTime']
                except Exception:
                    pass
            else:
                logger.warning("format %s has no such key: dbTime" % (format))

    return db_time, db_data


def get_db_info_str(db_time, db_data):
    """
    Convert db_time, db_data to strings.
    E.g. dbData="105077960", dbTime="251.42".

    :param db_time: time (s)
    :param db_data: long integer
    :return: db_time_s, db_data_s (strings)
    """

    try:
        zero = long(0)  # Python 2
    except Exception:
        zero = 0  # Python 3

    if db_data != zero:
        db_data_s = "%s" % (db_data)
    else:
        db_data_s = ""
    if db_time != 0:
        db_time_s = "%.2f" % (db_time)
    else:
        db_time_s = ""

    return db_time_s, db_data_s


def get_cpu_times(jobreport_dictionary):
    """
    Extract and add up the total CPU times from the job report.
    E.g. ('s', 5790L, 1.0).

    Note: this function is used with Event Service jobs

    :param jobreport_dictionary:
    :return: cpu_conversion_unit (unit), total_cpu_time, conversion_factor (output consistent with set_time_consumed())
    """

    try:
        total_cpu_time = long(0)  # Python 2
    except Exception:
        total_cpu_time = 0  # Python 3

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for format in list(executor_dictionary.keys()):  # "RAWtoESD", .., Python 2/3
            if 'cpuTime' in executor_dictionary[format]:
                try:
                    total_cpu_time += executor_dictionary[format]['cpuTime']
                except Exception:
                    pass
            else:
                logger.warning("format %s has no such key: cpuTime" % (format))

    conversion_factor = 1.0
    cpu_conversion_unit = "s"

    return cpu_conversion_unit, total_cpu_time, conversion_factor


def get_exit_info(jobreport_dictionary):
    """
    Return the exit code (exitCode) and exit message (exitMsg).
    E.g. (0, 'OK').

    :param jobreport_dictionary:
    :return: exit_code, exit_message
    """

    return jobreport_dictionary['exitCode'], jobreport_dictionary['exitMsg']


def cleanup_payload(workdir, outputfiles=[]):
    """
    Cleanup of payload (specifically AthenaMP) sub directories prior to log file creation.
    Also remove core dumps.

    :param workdir: working directory (string)
    :param outputfiles: list of output files
    :return:
    """

    remove_core_dumps(workdir)

    for ampdir in glob('%s/athenaMP-workers-*' % workdir):
        for (p, d, f) in os.walk(ampdir):
            for filename in f:
                if 'core' in filename or 'pool.root' in filename or 'tmp.' in filename:
                    path = os.path.join(p, filename)
                    path = os.path.abspath(path)
                    remove(path)
                for outfile in outputfiles:
                    if outfile in filename:
                        path = os.path.join(p, filename)
                        path = os.path.abspath(path)
                        remove(path)


def get_redundant_path():
    """
    Return the path to the file containing the redundant files and directories to be removed prior to log file creation.

    :return: file path (string).
    """

    filename = config.Pilot.redundant

    # correct /cvmfs if necessary
    if filename.startswith('/cvmfs') and os.environ.get('ATLAS_SW_BASE', False):
        filename = filename.replace('/cvmfs', os.environ.get('ATLAS_SW_BASE'))

    return filename


def get_redundants():
    """
    Get list of redundant files and directories (to be removed).
    The function will return the content of an external file. It that can't be read, then a list defined in this
    function will be returned instead. Any updates to the external file must be propagated to this function.

    :return: files and directories list
    """

    # try to read the list from the external file
    filename = get_redundant_path()
    if os.path.exists(filename) and False:  # do not use the cvmfs file since it is not being updated
        dir_list = read_list(filename)
        if dir_list:
            return dir_list

    logger.debug('list of redundant files could not be read from external file: %s (will use internal list)' % filename)

    # else return the following
    dir_list = ["AtlasProduction*",
                "AtlasPoint1",
                "AtlasTier0",
                "buildJob*",
                "CDRelease*",
                "csc*.log",
                "DBRelease*",
                "EvgenJobOptions",
                "external",
                "fort.*",
                "geant4",
                "geomDB",
                "geomDB_sqlite",
                "home",
                "o..pacman..o",
                "pacman-*",
                "python",
                "runAthena*",
                "share",
                "sources.*",
                "sqlite*",
                "sw",
                "tcf_*",
                "triggerDB",
                "trusted.caches",
                "workdir",
                "*.data*",
                "*.events",
                "*.py",
                "*.pyc",
                "*.root*",
                "JEM",
                "tmp*",
                "*.tmp",
                "*.TMP",
                "MC11JobOptions",
                "scratch",
                "*.writing",
                "pwg*",
                "pwhg*",
                "*PROC*",
                "madevent",
                "*proxy",
                "ckpt*",
                "*runcontainer*",
                "*job.log.tgz",
                "runGen-*",
                "runAthena-*",
                "pandawnutil/*",
                "src/*",
                "singularity_cachedir",
                "_joproxy15",
                "HAHM_*",
                "Process",
                "merged_lhef._0.events-new",
                "singularity/*",  # new
                "/cores",  # new
                "/work",  # new
                "/pilot2"]  # new

    return dir_list


def remove_archives(workdir):
    """
    Explicitly remove any soft linked archives (.a files) since they will be dereferenced by the tar command
    (--dereference option).

    :param workdir: working directory (string)
    :return:
    """

    matches = []
    for root, dirnames, filenames in os.walk(workdir):
        for filename in fnmatch.filter(filenames, '*.a'):
            matches.append(os.path.join(root, filename))
    for root, dirnames, filenames in os.walk(os.path.dirname(workdir)):
        for filename in fnmatch.filter(filenames, 'EventService_premerge_*.tar'):
            matches.append(os.path.join(root, filename))
    if matches != []:
        for f in matches:
            remove(f)


def cleanup_broken_links(workdir):
    """
    Run a second pass to clean up any broken links prior to log file creation.

    :param workdir: working directory (string)
    :return:
    """

    broken = []
    for root, dirs, files in os.walk(workdir):
        for filename in files:
            path = os.path.join(root, filename)
            if os.path.islink(path):
                target_path = os.readlink(path)
                # Resolve relative symlinks
                if not os.path.isabs(target_path):
                    target_path = os.path.join(os.path.dirname(path), target_path)
                if not os.path.exists(target_path):
                    broken.append(path)
            else:
                # If it's not a symlink we're not interested.
                continue

    if broken:
        for p in broken:
            remove(p)


def ls(workdir):
    cmd = 'ls -lF %s' % workdir
    ec, stdout, stderr = execute(cmd)
    logger.debug('%s:\n' % stdout + stderr)


def remove_special_files(workdir, dir_list, outputfiles):
    """
    Remove list of special files from the workdir.

    :param workdir: work directory (string).
    :param dir_list: list of special files (list).
    :param outputfiles: output files (list).
    :return:
    """

    # note: these should be partial file/dir names, not containing any wildcards
    exceptions_list = ["runargs", "runwrapper", "jobReport", "log."]

    to_delete = []
    for _dir in dir_list:
        files = glob(os.path.join(workdir, _dir))
        exclude = []

        if files:
            for exc in exceptions_list:
                for f in files:
                    if exc in f:
                        exclude.append(os.path.abspath(f))
            _files = []
            for f in files:
                if f not in exclude:
                    _files.append(os.path.abspath(f))
            to_delete += _files

    exclude_files = []
    for of in outputfiles:
        exclude_files.append(os.path.join(workdir, of))

    for f in to_delete:
        if f not in exclude_files:
            logger.debug('removing %s' % f)
            if os.path.isfile(f):
                remove(f)
            else:
                remove_dir_tree(f)


def remove_redundant_files(workdir, outputfiles=[], islooping=False):
    """
    Remove redundant files and directories prior to creating the log file.

    :param workdir: working directory (string).
    :param outputfiles: list of protected output files (list).
    :param islooping: looping job variable to make sure workDir is not removed in case of looping (boolean).
    :return:
    """

    logger.debug("removing redundant files prior to log creation")
    workdir = os.path.abspath(workdir)

    ls(workdir)

    # get list of redundant files and directories (to be removed)
    dir_list = get_redundants()

    # remove core and pool.root files from AthenaMP sub directories
    try:
        logger.debug('cleaning up payload')
        cleanup_payload(workdir, outputfiles)
    except Exception as e:
        logger.warning("failed to execute cleanup_payload(): %s" % e)

    # explicitly remove any soft linked archives (.a files) since they will be dereferenced by the tar command
    # (--dereference option)
    logger.debug('removing archives')
    remove_archives(workdir)

    # remove special files
    remove_special_files(workdir, dir_list, outputfiles)

    # run a second pass to clean up any broken links
    logger.debug('cleaning up broken links')
    cleanup_broken_links(workdir)

    # remove any present user workDir
    path = os.path.join(workdir, 'workDir')
    if os.path.exists(path) and not islooping:
        logger.debug('removing \'workDir\' from workdir=%s' % workdir)
        remove_dir_tree(path)

    # remove additional dirs
    additionals = ['singularity', 'pilot', 'cores']
    for additional in additionals:
        path = os.path.join(workdir, additional)
        if os.path.exists(path):
            logger.debug('removing \'%s\' from workdir=%s' % (additional, workdir))
            remove_dir_tree(path)

    ls(workdir)


def download_command(process, workdir):
    """
    Download the pre/postprocess commands if necessary.

    :param process: pre/postprocess dictionary.
    :param workdir: job workdir (string).
    :return: updated pre/postprocess dictionary.
    """

    cmd = process.get('command', '')

    # download the command if necessary
    if cmd.startswith('http'):
        # Try to download the trf (skip when user container is to be used)
        ec, diagnostics, cmd = get_analysis_trf(cmd, workdir)
        if ec != 0:
            logger.warning('cannot execute command due to previous error: %s' % cmd)
            return {}

        # update the preprocess command (the URL should be stripped)
        process['command'] = './' + cmd

    return process


def get_utility_commands(order=None, job=None):
    """
    Return a dictionary of utility commands and arguments to be executed in parallel with the payload.
    This could e.g. be memory and network monitor commands. A separate function can be used to determine the
    corresponding command setups using the utility command name.
    If the optional order parameter is set, the function should return the list of corresponding commands.
    E.g. if order=UTILITY_BEFORE_PAYLOAD, the function should return all commands that are to be executed before the
    payload. If order=UTILITY_WITH_PAYLOAD, the corresponding commands will be prepended to the payload execution
    string. If order=UTILITY_AFTER_PAYLOAD_STARTED, the commands that should be executed after the payload has been started
    should be returned. If order=UTILITY_WITH_STAGEIN, the commands that should be executed parallel with stage-in will
    be returned.

    FORMAT: {'command': <command>, 'args': <args>}

    :param order: optional sorting order (see pilot.util.constants).
    :param job: optional job object.
    :return: dictionary of utilities to be executed in parallel with the payload.
    """

    if order:
        if order == UTILITY_BEFORE_PAYLOAD and job and job.preprocess:
            if job.preprocess.get('command', ''):
                return download_command(job.preprocess, job.workdir)
        elif order == UTILITY_WITH_PAYLOAD:
            return {'command': 'NetworkMonitor', 'args': ''}
        elif order == UTILITY_AFTER_PAYLOAD_STARTED:
            cmd = config.Pilot.utility_after_payload_started
            if cmd:
                return {'command': cmd, 'args': ''}
        elif order == UTILITY_AFTER_PAYLOAD_STARTED2 and job and job.coprocess:
            # cmd = config.Pilot.utility_after_payload_started  DEPRECATED
            # if cmd:
            #    return {'command': cmd, 'args': ''}
            if job.coprocess.get('command', ''):
                return download_command(job.coprocess, job.workdir)
        elif order == UTILITY_AFTER_PAYLOAD and job and job.postprocess:
            if job.postprocess.get('command', ''):
                return download_command(job.postprocess, job.workdir)
        elif order == UTILITY_AFTER_PAYLOAD_FINISHED and job and job.postprocess:
            if job.postprocess.get('command', ''):
                return download_command(job.postprocess, job.workdir)
        elif order == UTILITY_WITH_STAGEIN:
            return {'command': 'Benchmark', 'args': ''}

    return {}


def get_utility_command_setup(name, job, setup=None):
    """
    Return the proper setup for the given utility command.
    If a payload setup is specified, then the utility command string should be prepended to it.

    :param name: name of utility (string).
    :param job: job object.
    :param setup: optional payload setup string.
    :return: utility command setup (string).
    """

    if name == 'MemoryMonitor':
        # must know if payload is running in a container or not (enables search for pid in ps output)
        use_container = job.usecontainer or 'runcontainer' in job.transformation
        dump_ps = True if "PRMON_DEBUG" in job.infosys.queuedata.catchall else False
        setup, pid = get_memory_monitor_setup(job.pid, job.pgrp, job.jobid, job.workdir, job.command, use_container=use_container,
                                              transformation=job.transformation, outdata=job.outdata, dump_ps=dump_ps)
        _pattern = r"([\S]+)\ ."
        pattern = re.compile(_pattern)
        _name = re.findall(pattern, setup.split(';')[-1])
        if _name:
            job.memorymonitor = _name[0]
        else:
            logger.warning('trf name could not be identified in setup string')

        # update the pgrp if the pid changed
        if job.pid != pid and pid != --1:
            logger.debug('updating pgrp=%d for pid=%d' % (job.pgrp, pid))
            try:
                job.pgrp = os.getpgid(pid)
            except Exception as e:
                logger.warning('os.getpgid(%d) failed with: %s' % (pid, e))
        return setup
    elif name == 'NetworkMonitor' and setup:
        return get_network_monitor_setup(setup, job)
    elif name == 'Prefetcher':
        return get_prefetcher_setup(job)
    elif name == 'Benchmark':
        return get_benchmark_setup(job)
    else:
        return ""


def get_utility_command_execution_order(name):
    """
    Should the given utility command be executed before or after the payload?

    :param name: utility name (string).
    :return: execution order constant.
    """

    # example implementation
    if name == 'NetworkMonitor':
        return UTILITY_WITH_PAYLOAD
    elif name == 'MemoryMonitor':
        return UTILITY_AFTER_PAYLOAD_STARTED
    else:
        logger.warning('unknown utility name: %s' % name)
        return UTILITY_AFTER_PAYLOAD_STARTED


def post_utility_command_action(name, job):
    """
    Perform post action for given utility command.

    :param name: name of utility command (string).
    :param job: job object.
    :return:
    """

    if name == 'NetworkMonitor':
        pass
    elif name == 'MemoryMonitor':
        post_memory_monitor_action(job)


def get_utility_command_kill_signal(name):
    """
    Return the proper kill signal used to stop the utility command.

    :param name: name of utility command (string).
    :return: kill signal
    """

    # note that the NetworkMonitor does not require killing (to be confirmed)
    sig = SIGUSR1 if name == 'MemoryMonitor' else SIGTERM
    return sig


def get_utility_command_output_filename(name, selector=None):
    """
    Return the filename to the output of the utility command.

    :param name: utility name (string).
    :param selector: optional special conditions flag (boolean).
    :return: filename (string).
    """

    if name == 'MemoryMonitor':
        filename = get_memory_monitor_summary_filename(selector=selector)
    else:
        filename = ""

    return filename


def verify_lfn_length(outdata):
    """
    Make sure that the LFNs are all within the allowed length.

    :param outdata: FileSpec object.
    :return: error code (int), diagnostics (string).
    """

    ec = 0
    diagnostics = ""
    max_length = 255

    # loop over all output files
    for fspec in outdata:
        if len(fspec.lfn) > max_length:
            diagnostics = "LFN too long (length: %d, must be less than %d characters): %s" % \
                          (len(fspec.lfn), max_length, fspec.lfn)
            ec = errors.LFNTOOLONG
            break

    return ec, diagnostics


def verify_ncores(corecount):
    """
    Verify that nCores settings are correct

    :param corecount: number of cores (int).
    :return:
    """

    try:
        del os.environ['ATHENA_PROC_NUMBER_JOB']
        logger.debug("unset existing ATHENA_PROC_NUMBER_JOB")
    except Exception:
        pass

    try:
        athena_proc_number = int(os.environ.get('ATHENA_PROC_NUMBER', None))
    except Exception:
        athena_proc_number = None

    # Note: if ATHENA_PROC_NUMBER is set (by the wrapper), then do not overwrite it
    # Otherwise, set it to the value of job.coreCount
    # (actually set ATHENA_PROC_NUMBER_JOB and use it if it exists, otherwise use ATHENA_PROC_NUMBER directly;
    # ATHENA_PROC_NUMBER_JOB will always be the value from the job definition)
    if athena_proc_number:
        logger.info("encountered a set ATHENA_PROC_NUMBER (%d), will not overwrite it" % athena_proc_number)
        logger.info('set ATHENA_CORE_NUMBER to same value as ATHENA_PROC_NUMBER')
        os.environ['ATHENA_CORE_NUMBER'] = "%s" % athena_proc_number
    else:
        os.environ['ATHENA_PROC_NUMBER_JOB'] = "%s" % corecount
        os.environ['ATHENA_CORE_NUMBER'] = "%s" % corecount
        logger.info("set ATHENA_PROC_NUMBER_JOB and ATHENA_CORE_NUMBER to %s (ATHENA_PROC_NUMBER will not be overwritten)" % corecount)


def verify_job(job):
    """
    Verify job parameters for specific errors.
    Note:
      in case of problem, the function should set the corresponding pilot error code using
      job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())

    :param job: job object
    :return: Boolean.
    """

    status = False

    # are LFNs of correct lengths?
    ec, diagnostics = verify_lfn_length(job.outdata)
    if ec != 0:
        logger.fatal(diagnostics)
        job.piloterrordiag = diagnostics
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(ec)
    else:
        status = True

    # check the ATHENA_PROC_NUMBER settings
    verify_ncores(job.corecount)

    return status


def update_stagein(job):
    """
    Skip DBRelease files during stage-in.

    :param job: job object.
    :return:
    """

    for fspec in job.indata:
        if 'DBRelease' in fspec.lfn:
            fspec.status = 'no_transfer'


def get_metadata(workdir):
    """
    Return the metadata from file.

    :param workdir: work directory (string)
    :return:
    """

    path = os.path.join(workdir, config.Payload.jobreport)
    metadata = read_file(path) if os.path.exists(path) else None
    logger.debug('metadata=%s' % str(metadata))

    return metadata


def should_update_logstash(frequency=10):
    """
    Should logstash be updated with prmon dictionary?

    :param frequency:
    :return: return True once per 'frequency' times.
    """

    from random import randint
    if randint(0, frequency - 1) == 0:
        return True
    else:
        return False


def update_server(job):
    """
    Perform any user specific server actions.

    E.g. this can be used to send special information to a logstash.

    :param job: job object.
    :return:
    """

    # attempt to read memory_monitor_output.txt and convert it to json
    if should_update_logstash():
        path = os.path.join(job.workdir, get_memory_monitor_output_filename())
        if os.path.exists(path):
            # convert memory monitor text output to json and return the selection (don't store it, log has already been created)
            metadata_dictionary = get_metadata_dict_from_txt(path, storejson=True, jobid=job.jobid)
            if metadata_dictionary:
                # the output was previously written to file, update the path and tell curl to send it
                new_path = update_extension(path=path, extension='json')
                #out = read_json(new_path)
                #logger.debug('prmon json=\n%s' % out)
                # logger.debug('final logstash prmon dictionary: %s' % str(metadata_dictionary))
                url = 'https://pilot.atlas-ml.org'  # 'http://collector.atlas-ml.org:80'
                #cmd = "curl --connect-timeout 20 --max-time 120 -H \"Content-Type: application/json\" -X POST -d \'%s\' %s" % \
                #      (str(metadata_dictionary).replace("'", '"'), url)
                # curl --connect-timeout 20 --max-time 120 -H "Content-Type: application/json" -X POST --upload-file test.json
                # https://pilot.atlas-ml.org
                cmd = "curl --connect-timeout 20 --max-time 120 -H \"Content-Type: application/json\" -X POST --upload-file %s %s" % (new_path, url)
                #cmd = "curl --connect-timeout 20 --max-time 120 -F 'data=@%s' %s" % (new_path, url)
                # send metadata to logstash
                try:
                    exit_code, stdout, stderr = execute(cmd, usecontainer=False)
                except Exception as e:
                    logger.warning('exception caught: %s' % e)
                else:
                    logger.debug('sent prmon JSON dictionary to logstash server')
                    logger.debug('stdout: %s' % stdout)
                    logger.debug('stderr: %s' % stderr)
            else:
                logger.warning('no prmon json available - cannot send anything to logstash server')
        else:
            logger.warning('path does not exist: %s' % path)
    else:
        logger.debug('no need to update logstash for this job')
