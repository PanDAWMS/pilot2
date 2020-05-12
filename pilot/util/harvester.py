#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import os
import os.path
import socket

from pilot.common.exception import FileHandlingFailure
from pilot.util.config import config
from pilot.util.filehandling import write_json, touch, remove, read_json, get_checksum_value
from pilot.util.timing import time_stamp

import logging
logger = logging.getLogger(__name__)


def dump(obj):
    """
    function for debugging - dumps object to sysout
    """
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


def is_harvester_mode(args):
    """
    Determine if the pilot is running in Harvester mode.
    :param args: Pilot arguments object.
    :return: Boolean.
    """

    if (args.harvester_workdir != '' or args.harvester_datadir != '') and not args.update_server:
        harvester = True
    elif (args.harvester_eventstatusdump != '' or args.harvester_workerattributes != '') and not args.update_server:
        harvester = True
    elif ('HARVESTER_ID' in os.environ or 'HARVESTER_WORKER_ID' in os.environ) and args.harvester_submitmode.lower() == 'push':
        harvester = True
    else:
        harvester = False

    return harvester


def get_job_request_file_name():
    """
    Return the name of the job request file as defined in the pilot config file.

    :return: job request file name.
    """

    #logger.debug('config.Harvester.__dict__ : {0}'.format(config.Harvester.__dict__))
    return os.path.join(os.environ['PILOT_HOME'], config.Harvester.job_request_file)


def remove_job_request_file():
    """
    Remove an old job request file when it is no longer needed.

    :return:
    """

    path = get_job_request_file_name()
    if os.path.exists(path):
        if remove(path) == 0:
            logger.info('removed %s' % path)
    else:
        logger.debug('there is no job request file')


def request_new_jobs(njobs=1):
    """
    Inform Harvester that the pilot is ready to process new jobs by creating a job request file with the desired
    number of jobs.

    :param njobs: Number of jobs. Default is 1 since on grids and clouds the pilot does not know how many jobs it can
    process before it runs out of time.
    :return:
    """

    path = get_job_request_file_name()
    dictionary = {'nJobs': njobs}

    # write it to file
    try:
        write_json(path, dictionary)
    except FileHandlingFailure:
        raise FileHandlingFailure


def kill_worker():
    """
    Create (touch) a kill_worker file in the pilot launch directory.
    This file will let Harverster know that the pilot has finished.

    :return:
    """

    touch(os.path.join(os.environ['PILOT_HOME'], config.Harvester.kill_worker_file))


def get_initial_work_report():
    """
    Prepare the work report dictionary.
    Note: the work_report should also contain all fields defined in parse_jobreport_data().

    :return: work report dictionary.
    """

    work_report = {'jobStatus': 'starting',
                   'messageLevel': logging.getLevelName(logger.getEffectiveLevel()),
                   'cpuConversionFactor': 1.0,
                   'cpuConsumptionTime': '',
                   'node': socket.gethostname(),
                   'workdir': '',
                   'timestamp': time_stamp(),
                   'endTime': '',
                   'transExitCode': 0,
                   'pilotErrorCode': 0,  # only add this in case of failure?
                   }

    return work_report


def get_event_status_file(args):
    """
    Return the name of the event_status.dump file as defined in the pilot config file
    and from the pilot arguments.

    :param args: Pilot arguments object.
    :return: event staus file name.
    """

    logger.debug('config.Harvester.__dict__ : {0}'.format(config.Harvester.__dict__))

    if args.harvester_workdir != '':
        work_dir = args.harvester_workdir
    else:
        work_dir = os.environ['PILOT_HOME']
    event_status_file = config.Harvester.stageoutnfile
    event_status_file = os.path.join(work_dir, event_status_file)
    logger.debug('event_status_file = {}'.format(event_status_file))

    return event_status_file


def get_worker_attributes_file(args):
    """
    Return the name of the worker attributes file as defined in the pilot config file
    and from the pilot arguments.

    :param args: Pilot arguments object.
    :return: worker attributes file name.
    """

    logger.debug('config.Harvester.__dict__ : {0}'.format(config.Harvester.__dict__))

    if args.harvester_workdir != '':
        work_dir = args.harvester_workdir
    else:
        work_dir = os.environ['PILOT_HOME']
    worker_attributes_file = config.Harvester.workerattributesfile
    worker_attributes_file = os.path.join(work_dir, worker_attributes_file)
    logger.debug('worker_attributes_file = {}'.format(worker_attributes_file))

    return worker_attributes_file


def findfile(path, name):
    """
    find the first instance of file in the directory tree

    :param path: directory tree to search
    :param name: name of the file to search

    :return: the path to the first instance of the file
    """

    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)
    return ''


def publish_stageout_files(job, event_status_file):
    """
    Publishing of work report to file.
    The work report dictionary should contain the fields defined in get_initial_work_report().

    :param args: Pilot arguments object.
    :param job: job object.
    :param event status file name:

    :return: Boolean. status of writing the file information to a json
    """

    # get the harvester workdir from the event_status_file
    work_dir = os.path.dirname(event_status_file)

    out_file_report = {}
    out_file_report[job.jobid] = []

    # first look at the logfile information (logdata) from the FileSpec objects
    for fspec in job.logdata:
        logger.debug("File {} will be checked and declared for stage out".format(fspec.lfn))
        # find the first instance of the file
        filename = os.path.basename(fspec.surl)
        path = findfile(work_dir, filename)
        logger.debug("Found File {} at path - {}".format(fspec.lfn, path))
        #
        file_desc = {}
        file_desc['type'] = fspec.filetype
        file_desc['path'] = path
        file_desc['guid'] = fspec.guid
        file_desc['fsize'] = fspec.filesize
        file_desc['chksum'] = get_checksum_value(fspec.checksum)
        logger.debug("File description - {} ".format(file_desc))
        out_file_report[job.jobid].append(file_desc)

    # Now look at the output file(s) information (outdata) from the FileSpec objects
    for fspec in job.outdata:
        logger.debug("File {} will be checked and declared for stage out".format(fspec.lfn))
        if fspec.status != 'transferred':
            logger.debug('will not add the output file to the json since it was not produced or transferred')
        else:
            # find the first instance of the file
            filename = os.path.basename(fspec.surl)
            path = findfile(work_dir, filename)
            if not path:
                logger.warning('file %s was not found - will not be added to json')
            else:
                logger.debug("Found File {} at path - {}".format(fspec.lfn, path))
                #
                file_desc = {}
                file_desc['type'] = fspec.filetype
                file_desc['path'] = path
                file_desc['guid'] = fspec.guid
                file_desc['fsize'] = fspec.filesize
                file_desc['chksum'] = get_checksum_value(fspec.checksum)
                logger.debug("File description - {} ".format(file_desc))
                out_file_report[job.jobid].append(file_desc)

    if out_file_report[job.jobid]:
        if write_json(event_status_file, out_file_report):
            logger.debug('Stagout declared in: {0}'.format(event_status_file))
            logger.debug('Report for stageout: {}'.format(out_file_report))
            return True
        else:
            logger.debug('Failed to declare stagout in: {0}'.format(event_status_file))
            return False
    else:
        logger.debug('No Report for stageout')
        return False


def publish_work_report(work_report=None, worker_attributes_file="worker_attributes.json"):
    """
    Publishing of work report to file.
    The work report dictionary should contain the fields defined in get_initial_work_report().

    :param work_report: work report dictionary.
    :param worker_attributes_file:
    :raises FileHandlingFailure: in case of IOError.
    :return: True or False
    """

    if work_report:
        try:
            work_report['timestamp'] = time_stamp()
            if "outputfiles" in work_report:
                del(work_report["outputfiles"])
            if "inputfiles" in work_report:
                del (work_report["inputfiles"])
            if "xml" in work_report:
                del (work_report["xml"])
            if write_json(worker_attributes_file, work_report):
                logger.info("work report published: {0}".format(work_report))
                return True
            else:
                logger.error("work report publish failed: {0}".format(work_report))
                return False
        except IOError:
            logger.error("job report copy failed")
            return False
        except Exception as e:
            logger.error("write json file failed: {0}".format(e))
            return False
    else:
        # No work_report return False
        return False


def publish_job_report(job, args, job_report_file="jobReport.json"):
    """
    Copy job report file to make it accessible by Harvester. Shrink job report file.

    :param job: job object.
    :param args: Pilot arguments object.
    :param job_report_file: name of job report (string).
    :raises FileHandlingFailure: in case of IOError.
    :return True or False
    """

    src_file = os.path.join(job.workdir, job_report_file)
    dst_file = os.path.join(args.harvester_workdir, job_report_file)

    try:
        logger.info(
            "copy of payload report [{0}] to access point: {1}".format(job_report_file, args.harvester_workdir))
        # shrink jobReport
        job_report = read_json(src_file)
        if 'executor' in job_report:
            for executor in job_report['executor']:
                if 'logfileReport' in executor:
                    executor['logfileReport'] = {}

        if write_json(dst_file, job_report):
            return True
        else:
            return False

    except IOError:
        logger.error("job report copy failed")
        return False


def parse_job_definition_file(filename):
    """
    This function parses the Harvester job definition file and re-packages the job definition dictionaries.
    The format of the Harvester job definition dictionary is:
    dict = { job_id: { key: value, .. }, .. }
    The function returns a list of these dictionaries each re-packaged as
    dict = { key: value } (where the job_id is now one of the key-value pairs: 'jobid': job_id)

    :param filename: file name (string).
    :return: list of job definition dictionaries.
    """

    job_definitions_list = []

    # re-package dictionaries
    job_definitions_dict = read_json(filename)
    if job_definitions_dict:
        for job_id in job_definitions_dict:
            res = {'jobid': job_id}
            res.update(job_definitions_dict[job_id])
            job_definitions_list.append(res)

    return job_definitions_list
