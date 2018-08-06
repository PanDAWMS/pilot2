#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2018

import os
import re
import fnmatch
from collections import defaultdict
from glob import glob
from signal import SIGTERM, SIGUSR1

from pilot.common.exception import TrfDownloadFailure, PilotException
from pilot.user.atlas.setup import should_pilot_prepare_asetup, get_asetup, get_asetup_options, is_standard_atlas_job,\
    set_inds, get_analysis_trf, get_payload_environment_variables
from pilot.user.atlas.utilities import get_memory_monitor_setup, get_network_monitor_setup, post_memory_monitor_action,\
    get_memory_monitor_summary_filename, get_prefetcher_setup, get_benchmark_setup
from pilot.util.auxiliary import get_logger
from pilot.util.constants import UTILITY_BEFORE_PAYLOAD, UTILITY_WITH_PAYLOAD, UTILITY_AFTER_PAYLOAD,\
    UTILITY_WITH_STAGEIN
from pilot.util.container import execute
from pilot.util.filehandling import remove, get_guid

from pilot.info import FileSpec

import logging
logger = logging.getLogger(__name__)


def get_payload_command(job):
    """
    Return the full command for execuring the payload, including the sourcing of all setup files and setting of
    environment variables.

    :param job: job object.
    :raises PilotException: TrfDownloadFailure.
    :return: command (string).
    """

    log = get_logger(job.jobid)

    # Should the pilot do the asetup or do the jobPars already contain the information?
    prepareasetup = should_pilot_prepare_asetup(job.noexecstrcnv, job.jobparams)

    # Is it a user job or not?
    userjob = job.is_analysis()

    # Get the platform value
    # platform = job.infosys.queuedata.platform

    cmd = get_setup_command(job, prepareasetup)

    if is_standard_atlas_job(job.swrelease):

        # Normal setup (production and user jobs)
        log.info("preparing normal production/analysis job setup command")

        if userjob:
            # set the INDS env variable (used by runAthena)
            set_inds(job.datasetin)  # realDatasetsIn

            # Try to download the trf
            ec, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir)
            if ec != 0:
                raise TrfDownloadFailure(diagnostics)
            else:
                log.debug('user analysis trf: %s' % trf_name)

            if prepareasetup:
                _cmd = get_analysis_run_command(job, trf_name)
            else:
                _cmd = job.jobparams

            # Correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make)
            cmd += "; " + add_makeflags(job.corecount, "") + _cmd
        else:
            # Add Database commands if they are set by the local site
            cmd += os.environ.get('PILOT_DB_LOCAL_SETUP_CMD', '')
            # Add the transform and the job parameters (production jobs)
            if prepareasetup:
                cmd += "; %s %s" % (job.transformation, job.jobparams)
            else:
                cmd += "; " + job.jobparams

        cmd = cmd.replace(';;', ';')

    else:  # Generic, non-ATLAS specific jobs, or at least a job with undefined swRelease

        log.info("generic job (non-ATLAS specific or with undefined swRelease)")

        if userjob:
            # Try to download the trf
            ec, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir)
            if ec != 0:
                raise TrfDownloadFailure(diagnostics)
            else:
                log.debug('user analysis trf: %s' % trf_name)

            if prepareasetup:
                _cmd = get_analysis_run_command(job, trf_name)
            else:
                _cmd = job.jobparams

            # correct for multi-core if necessary (especially important in case coreCount=1 to limit parallel make)
            cmd += "; " + add_makeflags(job.corecount, "") + _cmd

        elif verify_release_string(job.homepackage) != 'NULL' and job.homepackage != ' ':
            if prepareasetup:
                cmd = "python %s/%s %s" % (job.homepackage, job.transformation, job.jobparams)
            else:
                cmd = job.jobparams
        else:
            if prepareasetup:
                cmd = "python %s %s" % (job.transformation, job.jobparams)
            else:
                cmd = job.jobparams

    site = os.environ.get('PILOT_SITENAME', '')
    variables = get_payload_environment_variables(cmd, job.jobid, job.taskid, job.processingtype, site, userjob)
    cmd = ''.join(variables) + cmd

    log.info('payload run command: %s' % cmd)

    return cmd


def get_setup_command(job, prepareasetup):
    """
    Return the path to asetup command, the asetup command itself and add the options (if desired).
    If prepareasetup is False, the function will only return the path to the asetup script. It is then assumed
    to be part of the job parameters.

    :param job: job object.
    :param prepareasetup: should the pilot prepare the asetup command itself? boolean.
    :return:
    """

    # return immediately if there is no release
    if job.swrelease == 'NULL':
        return ""

    # Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    cmd = get_asetup(asetup=prepareasetup)

    if prepareasetup:
        options = get_asetup_options(job.swrelease, job.homepackage)
        asetupoptions = " " + options + " --platform " + job.platform

        # Always set the --makeflags option (to prevent asetup from overwriting it)
        asetupoptions += ' --makeflags=\"$MAKEFLAGS\"'

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
                cmd += 'export MAKEFLAGS="-j%d QUICK=1 -l1";' % (core_count)

    # make sure that MAKEFLAGS is always set
    if "MAKEFLAGS=" not in cmd:
        cmd += 'export MAKEFLAGS="-j1 QUICK=1 -l1";'

    return cmd


def get_analysis_run_command(job, trf_name):
    """
    Return the proper run command for the user job.

    Example output: export X509_USER_PROXY=<..>;./runAthena <job parameters> --usePFCTurl --directIn

    :param job: job object.
    :param trf_name: name of the transform that will run the job (string).
    :return: command (string).
    """

    cmd = ""

    log = get_logger(job.jobid)

    # get relevant file transfer info
    use_copy_tool, use_direct_access, use_pfc_turl = get_file_transfer_info(job.transfertype,
                                                                            job.is_build_job(),
                                                                            job.infosys.queuedata)
    # add the user proxy
    if 'X509_USER_PROXY' in os.environ:
        cmd += 'export X509_USER_PROXY=%s;' % os.environ.get('X509_USER_PROXY')
    else:
        log.warning("could not add user proxy to the run command (proxy does not exist)")

    # set up analysis trf
    cmd += './%s %s' % (trf_name, job.jobparams)

    # add control options for PFC turl and direct access
    if use_pfc_turl and '--usePFCTurl' not in cmd:
        cmd += ' --usePFCTurl'
    if use_direct_access and '--directIn' not in cmd:
        cmd += ' --directIn'

    # update the payload command for forced accessmode
    cmd = update_forced_accessmode(log, cmd, job.transfertype, job.jobparams, trf_name)

    # add guids when needed
    # get the correct guids list (with only the direct access files)
    if not job.is_build_job():
        _guids = get_guids_from_jobparams(job.jobparams, job.infiles, job.infilesguids)
        if _guids:
            cmd += ' --inputGUIDs \"%s\"' % (str(_guids))

    return cmd


def update_forced_accessmode(log, cmd, transfertype, jobparams, trf_name):
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
        for _mode in _accessmode_dic.keys():
            if _mode in jobparams:
                # any accessmode set in jobPars should overrule schedconfig
                log.info("enforcing %s" % _accessmode_dic[_mode][0])
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
            log.info('forced copy tool usage selected')
            # remove again the "--directIn"
            if "directIn" in cmd:
                cmd = cmd.replace(' --directIn', ' ')
        elif accessmode_directin:
            log.info('forced direct access usage selected')
            if "directIn" not in cmd:
                cmd += ' --directIn'
        else:
            log.warning('neither forced copy tool usage nor direct access was selected')

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
        match = re.search("-i ([A-Za-z0-9.\[\],_-]+) ", jobparams)
        if match is not None:
            compactinfiles = match.group(1)
            match = re.search('(.*)\[(.+)\](.*)\[(.+)\]', compactinfiles)
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


def get_file_transfer_info(transfertype, is_a_build_job, queuedata):
    """
    Return information about desired file transfer.

    :param transfertype:
    :param is_a_build_job: boolean.
    :param queuedata: infosys queuedata object from job object.
    :return: use copy tool (boolean), use direct access (boolean), use PFC Turl (boolean).
    """

    use_copy_tool = True
    use_direct_access = False
    use_pfc_turl = False

    # check with schedconfig
    if (queuedata.direct_access_lan or queuedata.direct_access_wan or transfertype == 'direct') and not is_a_build_job:
        use_copy_tool = False
        use_direct_access = True
        use_pfc_turl = True

    return use_copy_tool, use_direct_access, use_pfc_turl


def update_job_data(job):  # noqa: C901
    """
    This function can be used to update/add data to the job object.
    E.g. user specific information can be extracted from other job object fields. In the case of ATLAS, information
    is extracted from the metaData field and added to other job object fields.

    :param job: job object
    :return:
    """

    ## comment from Alexey:
    ## it would be better to reallocate this logic (as well as parse metadata values)directly to Job object
    ## since in general it's Job related part
    ## later on once we introduce VO specific Job class (inherited from JobData) this can be easily customized

    log = get_logger(job.jobid)

    stageout = "all"

    # handle any error codes
    if 'exeErrorCode' in job.metadata:
        job.exeerrorcode = job.metadata['exeErrorCode']
        if job.exeerrorcode == 0:
            stageout = "all"
        else:
            log.info('payload failed: exeErrorCode=%d' % job.exeerrorcode)
            stageout = "log"
    if 'exeErrorDiag' in job.metadata:
        job.exeerrordiag = job.metadata['exeErrorDiag']
        if job.exeerrordiag:
            log.warning('payload failed: exeErrorDiag=%s' % job.exeerrordiag)

    # determine what should be staged out
    job.stageout = stageout  # output and log file or only log file

    # extract the number of events
    job.nevents = get_number_of_events(job.metadata)

    work_attributes = None
    try:
        work_attributes = parse_jobreport_data(job.metadata)
    except Exception as e:
        log.warning('failed to parse job report: %s' % e)

    log.info('work_attributes = %s' % work_attributes)

    # extract output files from the job report, in case the trf has created additional (overflow) files
    # also make sure all guids are assigned (use job report value if present, otherwise generate the guid)
    if job.metadata:
        data = dict([e.lfn, e] for e in job.outdata)
        extra = []

        for dat in job.metadata.get('files', {}).get('output', []):
            for fdat in dat.get('subFiles', []):
                lfn = fdat['name']

                # verify the guid if the lfn is known
                if lfn in data:
                    data[lfn].guid = fdat['file_guid']
                    logger.info('set guid=%s for lfn=%s (value taken from job report)' % (data[lfn].guid, lfn))
                else:  # found new entry, create filespec
                    if not job.outdata:
                        raise PilotException("job.outdata is empty, will not be able to construct filespecs")
                    kw = {'lfn': lfn,
                          'scope': job.outdata[0].scope,  ## take value from 1st output file?
                          'guid': fdat['file_guid'],
                          'filesize': fdat['file_size'],
                          'dataset': dat.get('dataset') or job.outdata[0].dataset  ## take value from 1st output file?
                          }
                    spec = FileSpec(type='output', **kw)
                    extra.append(spec)

        if extra:
            log.info('found extra output files to be added for stage-out: extra=%s' % extra)
            job.outdata.extend(extra)
    else:
        log.warning('job.metadata not set')

    ## validate output data (to be moved into the JobData)
    ## warning: do no execute this code unless guid lookup in job report has failed - pilot should only generate guids
    ## if they are not present in job report
    for dat in job.outdata:
        if not dat.guid:
            dat.guid = get_guid()
            log.warning('guid not set: generated guid=%s for lfn=%s' % (dat.guid, dat.lfn))


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
    core_count = "undef"
    work_attributes["n_events"] = "None"
    work_attributes["__db_time"] = "None"
    work_attributes["__db_data"] = "None"

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
            else:
                return

    if 'ATHENA_PROC_NUMBER' in os.environ:
        work_attributes['core_count'] = os.environ['ATHENA_PROC_NUMBER']
        core_count = os.environ['ATHENA_PROC_NUMBER']

    dq = DictQuery(job_report)
    dq.get("resource/transform/processedEvents", work_attributes, "n_events")
    dq.get("resource/transform/cpuTimeTotal", work_attributes, "cpuConsumptionTime")
    dq.get("resource/machine/node", work_attributes, "node")
    dq.get("resource/machine/model_name", work_attributes, "cpuConsumptionUnit")
    dq.get("resource/dbTimeTotal", work_attributes, "__db_time")
    dq.get("resource/dbDataTotal", work_attributes, "__db_data")
    dq.get("exitCode", work_attributes, "transExitCode")
    dq.get("exitMsg", work_attributes, "exeErrorDiag")
    dq.get("files/input/subfiles", work_attributes, "nInputFiles")

    if 'resource' in job_report and 'executor' in job_report['resource']:
        j = job_report['resource']['executor']
        exc_report = []
        fin_report = defaultdict(int)
        for v in filter(lambda d: 'memory' in d and ('Max' or 'Avg' in d['memory']), j.itervalues()):
            if 'Avg' in v['memory']:
                exc_report.extend(v['memory']['Avg'].items())
            if 'Max' in v['memory']:
                exc_report.extend(v['memory']['Max'].items())
        for x in exc_report:
            fin_report[x[0]] += x[1]
        work_attributes.update(fin_report)

    if 'files' in job_report and 'input' in job_report['files'] and 'subfiles' in job_report['files']['input']:
                work_attributes['nInputFiles'] = len(job_report['files']['input']['subfiles'])

    workdir_size = get_workdir_size()
    work_attributes['jobMetrics'] = 'coreCount=%s nEvents=%s dbTime=%s dbData=%s workDirSize=%s' % \
                                    (core_count,
                                        work_attributes["n_events"],
                                        work_attributes["__db_time"],
                                        work_attributes["__db_data"],
                                        workdir_size)
    del(work_attributes["__db_time"])
    del(work_attributes["__db_data"])

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


def get_number_of_events(jobreport_dictionary):
    """
    Extract the number of events from the job report.

    :param jobreport_dictionary:
    :return:
    """

    nevents = {}  # FORMAT: { format : total_events, .. }
    nmax = 0

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for format in executor_dictionary.keys():  # "RAWtoESD", ..
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
    db_data = 0L

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for format in executor_dictionary.keys():  # "RAWtoESD", ..
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

    if db_data != 0L:
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

    total_cpu_time = 0L

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for format in executor_dictionary.keys():  # "RAWtoESD", ..
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

    :param workdir: working directory (string)
    :param outputfiles: list of output files
    :return:
    """

    for ampdir in glob('%s/athenaMP-workers-*' % (workdir)):
        for (p, d, f) in os.walk(ampdir):
            for filename in f:
                if 'core' in filename or 'tmp.' in filename:
                    path = os.path.join(p, filename)
                    path = os.path.abspath(path)
                    remove(path)
                for outfile in outputfiles:
                    if outfile in filename:
                        path = os.path.join(p, filename)
                        path = os.path.abspath(path)
                        remove(path)


def get_redundants():
    """
    Get list of redundant files and directories (to be removed).
    :return: files and directories list
    """

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
                "ckpt*"]

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


def remove_redundant_files(workdir, outputfiles=[]):
    """
    Remove redundant files and directories prior to creating the log file.

    :param workdir: working directory (string).
    :param outputfiles: list of output files.
    :return:
    """

    logger.info("removing redundant files prior to log creation")

    workdir = os.path.abspath(workdir)

    # get list of redundant files and directories (to be removed)
    dir_list = get_redundants()

    # remove core and pool.root files from AthenaMP sub directories
    try:
        cleanup_payload(workdir, outputfiles)
    except Exception as e:
        logger.warning("failed to execute cleanup_payload(): %s" % e)

    # explicitly remove any soft linked archives (.a files) since they will be dereferenced by the tar command
    # (--dereference option)
    remove_archives(workdir)

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
            remove(f)

    # run a second pass to clean up any broken links
    cleanup_broken_links(workdir)


def get_utility_commands_list(order=None):
    """
    Return a list of utility commands to be executed in parallel with the payload.
    This could e.g. be memory and network monitor commands. A separate function can be used to determine the
    corresponding command setups using the utility command name.
    If the optional order parameter is set, the function should return the list of corresponding commands.
    E.g. if order=UTILITY_BEFORE_PAYLOAD, the function should return all commands that are to be executed before the
    payload. If order=UTILITY_WITH_PAYLOAD, the corresponding commands will be prepended to the payload execution
    string. If order=UTILITY_AFTER_PAYLOAD, the commands that should be executed after the payload has been started
    should be returned. If order=UTILITY_WITH_STAGEIN, the commands that should be executed parallel with stage-in will
    be returned.

    :param order: optional sorting order (see pilot.util.constants)
    :return: list of utilities to be executed in parallel with the payload.
    """

    if order:
        if order == UTILITY_BEFORE_PAYLOAD:
            return ['Prefetcher']
        elif order == UTILITY_WITH_PAYLOAD:
            return ['NetworkMonitor']
        elif order == UTILITY_AFTER_PAYLOAD:
            return ['MemoryMonitor']
        elif order == UTILITY_WITH_STAGEIN:
            return ['Benchmark']
    return []


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
        return get_memory_monitor_setup(job)
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
        return UTILITY_AFTER_PAYLOAD
    else:
        logger.warning('unknown utility name: %s' % name)
        return UTILITY_AFTER_PAYLOAD


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

    :param name:
    :return: kill signal
    """

    if name == 'MemoryMonitor':
        sig = SIGUSR1
    else:
        # note that the NetworkMonitor does not require killing (to be confirmed)
        sig = SIGTERM

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
