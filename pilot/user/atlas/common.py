#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os
import fnmatch
from collections import defaultdict
from glob import glob
from signal import SIGTERM, SIGUSR1

# from pilot.common.exception import PilotException
from pilot.util.constants import UTILITY_WITH_PAYLOAD, UTILITY_AFTER_PAYLOAD
from pilot.util.container import execute
from pilot.user.atlas.setup import should_pilot_prepare_asetup, get_asetup, get_asetup_options, is_standard_atlas_job
from pilot.util.filehandling import remove
from pilot.user.atlas.utilities import get_memory_monitor_setup, get_network_monitor_setup, post_memory_monitor_action,\
    get_memory_monitor_summary_filename

import logging
logger = logging.getLogger(__name__)


def get_payload_command(job):
    """
    Return the full command for execuring the payload, including the sourcing of all setup files and setting of
    environment variables.

    :param job: job object
    :return: command (string)
    """

    # Should the pilot do the asetup or do the jobPars already contain the information?
    prepareasetup = should_pilot_prepare_asetup(job.noexecstrcnv, job.jobparams)

    # Is it a user job or not?
    userjob = job.is_analysis()

    # Get the platform value
    # platform = job.infosys.queuedata.platform

    # Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    asetuppath = get_asetup(asetup=prepareasetup)
    asetupoptions = " "

    if is_standard_atlas_job(job.swrelease):

        # Normal setup (production and user jobs)
        logger.info("preparing normal production/analysis job setup command")

        cmd = asetuppath
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

            if userjob:
                pass
            else:
                # Add Database commands if they are set by the local site
                cmd += os.environ.get('PILOT_DB_LOCAL_SETUP_CMD', '')
                # Add the transform and the job parameters (production jobs)
                if prepareasetup:
                    cmd += ";%s %s" % (job.transformation, job.jobparams)
                else:
                    cmd += "; " + job.jobparams

            cmd = cmd.replace(';;', ';')

    else:  # Generic, non-ATLAS specific jobs, or at least a job with undefined swRelease

        logger.info("generic job (non-ATLAS specific or with undefined swRelease)")

        cmd = ""

    return cmd


def update_job_data(job):
    """
    This function can be used to update/add data to the job object.
    E.g. user specific information can be extracted from other job object fields. In the case of ATLAS, information
    is extracted from the metaData field and added to other job object fields.

    :param job: job object
    :return:
    """

    stageout = "all"

    # handle any error codes
    if 'exeErrorCode' in job.metadata:
        job.exeerrorcode = job.metadata['exeErrorCode']
        if job.exeerrorcode == 0:
            stageout = "all"
        else:
            logger.info('payload failed: exeErrorCode=%d' % job.exeerrorcode)
            stageout = "log"
    if 'exeErrorDiag' in job.metadata:
        job.exeerrordiag = job.metadata['exeErrorDiag']
        if job.exeerrordiag != "":
            logger.warning('payload failed: exeErrorDiag=%s' % job.exeerrordiag)

    # determine what should be staged out
    job.stageout = stageout  # output and log file or only log file

    # extract the number of events
    job.nevents = get_number_of_events(job.metadata)

    try:
        work_attributes = parse_jobreport_data(job.metadata)
    except Exception as e:
        logger.warning('failed to parse job report: %s' % e)
    else:
        logger.info('work_attributes = %s' % str(work_attributes))


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
    work_attributes["n_events"] = "undef"
    work_attributes["__db_time"] = "undef"
    work_attributes["__db_data"] = "undef"

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
    dq.get("exitCode", work_attributes, "exeErrorCode")
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
                    print executor_dictionary[format]['nevents']
                    nevents[format] += executor_dictionary[format]['nevents']
                else:
                    nevents[format] = executor_dictionary[format]['nevents']
            else:
                logger.warning("format %s has no such key: nevents" % (format))

    # Now find the largest number of events among the different formats
    if nevents != {}:
        try:
            nmax = max(nevents.values())
        except Exception, e:
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

    :param jobreport_dictionary:
    :return: db_time, db_data
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
                "jobState-*-test.pickle",
                "*.writing",
                "pwg*",
                "pwhg*",
                "*PROC*",
                "madevent",
                "HPC",
                "objectstore*.json",
                "saga",
                "radical",
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
    except Exception, e:
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
    should be returned.

    :param order: optional sorting order (see pilot.util.constants)
    :return: list of utilities to be executed in parallel with the payload.
    """

    if order:
        if order == UTILITY_WITH_PAYLOAD:
            return ['NetworkMonitor']
        elif order == UTILITY_AFTER_PAYLOAD:
            return ['MemoryMonitor']

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


def get_utility_command_output_filename(name):
    """
    Return the filename to the output of the utility command.

    :param name: utility name (string).
    :return: filename (string).
    """

    if name == 'MemoryMonitor':
        filename = get_memory_monitor_summary_filename()
    else:
        filename = ""

    return filename
