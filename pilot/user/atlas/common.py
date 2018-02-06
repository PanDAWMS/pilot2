#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os

# from pilot.common.exception import PilotException
from pilot.user.atlas.setup import should_pilot_prepare_asetup, is_user_analysis_job, get_platform, get_asetup, \
    get_asetup_options, is_standard_atlas_job

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
    prepareasetup = should_pilot_prepare_asetup(job.get('noExecStrCnv', None), job['jobPars'])

    # Is it a user job or not?
    userjob = is_user_analysis_job(job['transformation'])

    # Get the platform value
    platform = get_platform(job['cmtConfig'])

    # Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    asetuppath = get_asetup(asetup=prepareasetup)
    asetupoptions = " "

    if is_standard_atlas_job(job['swRelease']):

        # Normal setup (production and user jobs)
        logger.info("preparing normal production/analysis job setup command")

        cmd = asetuppath
        if prepareasetup:
            options = get_asetup_options(job['swRelease'], job['homepackage'])
            asetupoptions = " " + options + " --platform " + platform

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
                    cmd += ";%s %s" % (job['transformation'], job['jobPars'])
                else:
                    cmd += "; " + job['jobPars']

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
    if 'exeErrorCode' in job['metaData']:
        job['exeErrorCode'] = job['metaData']['exeErrorCode']
        if job['exeErrorCode'] == 0:
            stageout = "all"
        else:
            logger.info('payload failed: exeErrorCode=%d' % job['exeErrorCode'])
            stageout = "log"
    if 'exeErrorDiag' in job['metaData']:
        job['exeErrorDiag'] = job['metaData']['exeErrorDiag']
        if job['exeErrorDiag'] != "":
            logger.warning('payload failed: exeErrorDiag=%s' % job['exeErrorDiag'])

    # determine what should be staged out
    job['stageout'] = stageout  # output and log file or only log file

    # extract the number of events
    job['nEvents'] = get_number_of_events(job['metaData'])


def get_number_of_events(jobreport_dictionary):
    """
    Extract the number of events from the job report.

    :param jobreport_dictionary:
    :return:
    """

    nevents = {}  # FORMAT: { format : total_events, .. }

    if jobreport_dictionary != {}:

        if 'resource' in jobreport_dictionary:
            resource_dictionary = jobreport_dictionary['resource']
            if 'executor' in resource_dictionary:
                executor_dictionary = resource_dictionary['executor']
                for format in executor_dictionary.keys():  # "RAWtoESD", ..
                    if 'nevents' in executor_dictionary[format]:
                        if nevents.has_key(format):
                            print executor_dictionary[format]['nevents']
                            nevents[format] += executor_dictionary[format]['nevents']
                        else:
                            nevents[format] = executor_dictionary[format]['nevents']
                    else:
                        logger.warning("format %s has no such key: nevents" % (format))
            else:
                logger.warning("no such key: executor")
        else:
            logger.warning("no such key: resource")

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

    :param jobreport_dictionary:
    :return: db_time_s, db_data_s [converted strings, from e.g. "dbData=105077960 dbTime=251.42"]
    """

    db_time = 0
    db_data = 0L

    if jobreport_dictionary != {}:

        if 'resource' in jobreport_dictionary:
            resource_dictionary = jobreport_dictionary['resource']
            if 'executor' in resource_dictionary:
                executor_dictionary = resource_dictionary['executor']
                for format in executor_dictionary.keys():  # "RAWtoESD", ..
                    if 'dbData' in executor_dictionary[format]:
                        try:
                            db_data += executor_dictionary[format]['dbData']
                        except:
                            pass
                    else:
                        logger.warning("format %s has no such key: dbData" % (format))
                    if executor_dictionary[format].has_key('dbTime'):
                        try:
                            db_time += executor_dictionary[format]['dbTime']
                        except:
                            pass
                    else:
                        logger.warning("format %s has no such key: dbTime" % (format))
            else:
                logger.warning("no such key: executor")
        else:
            logger.warning("no such key: resource")

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
    Note: this function is used with Event Service jobs

    :param jobreport_dictionary:
    :return: cpu_conversion_unit (unit), total_cpu_time, conversion_factor (output consistent with set_time_consumed())
    """

    total_cpu_time = 0L

    if jobreport_dictionary != {}:
        if 'resource' in jobreport_dictionary:
            resource_dictionary = jobreport_dictionary['resource']
            if 'executor' in resource_dictionary:
                executor_dictionary = resource_dictionary['executor']
                for format in executor_dictionary.keys():  # "RAWtoESD", ..
                    if 'cpuTime' in executor_dictionary[format]:
                        try:
                            total_cpu_time += executor_dictionary[format]['cpuTime']
                        except:
                            pass
                    else:
                        logger.warning("format %s has no such key: cpuTime" % (format))
            else:
                logger.warning("no such key: executor")
        else:
            logger.warning("no such key: resource")

    conversion_factor = 1.0
    cpu_conversion_unit = "s"

    return cpu_conversion_unit, total_cpu_time, conversion_factor
