#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from os import environ
from os.path import join, exists
from socket import gethostname

from pilot.common.exception import FileHandlingFailure
from pilot.util.config import config
from pilot.util.filehandling import write_json, touch, remove, read_json
from pilot.util.timing import time_stamp

import logging
logger = logging.getLogger(__name__)


def is_harvester_mode(args):
    """
    Determine if the pilot is running in Harvester mode.
    :param args:
    :return:
    """

    if (args.harvester_workdir != '' or args.harvester_datadir != '' or args.harvester_eventstatusdump != '' or
            args.harvester_workerattributes != '') and not args.update_server:
        harvester = True
    elif 'HARVESTER_ID' in environ or 'HARVESTER_WORKER_ID' in environ:
        harvester = True
    else:
        harvester = False

    return harvester


def get_job_request_file_name():
    """
    Return the name of the job request file as defined in the pilot config file.

    :return: job request file name.
    """

    return join(environ['PILOT_HOME'], config.Harvester.job_request_file)


def remove_job_request_file():
    """
    Remove an old job request file when it is no longer needed.

    :return:
    """

    path = get_job_request_file_name()
    if exists(path):
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

    touch(join(environ['PILOT_HOME'], config.Harvester.kill_worker_file))


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
                   'node': gethostname(),
                   'workdir': '',
                   'timestamp': time_stamp(),
                   'endTime': '',
                   'transExitCode': 0,
                   'pilotErrorCode': 0,  # only add this in case of failure?
                   }

    return work_report


def publish_work_report(work_report=None, worker_attributes_file="worker_attributes.json"):
    """
    Publishing of work report to file.
    The work report dictionary should contain the fields defined in get_initial_work_report().

    :param work_report: work report dictionary.
    :param worker_attributes_file:
    :return:
    """

    if work_report:
        work_report['timestamp'] = time_stamp()
        if "outputfiles" in work_report:
            del(work_report["outputfiles"])
        if "inputfiles" in work_report:
            del (work_report["inputfiles"])
        if write_json(worker_attributes_file, work_report):
            logger.info("work report published: {0}".format(work_report))


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
