#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

import time

from pilot.common.errorcodes import ErrorCodes
from pilot.info import JobData
from pilot.util.auxiliary import set_pilot_state

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def declare_failed_by_kill(job, queue, sig):
    """
    Declare the job failed by a kill signal and put it in a suitable failed queue.
    E.g. queue=queues.failed_data_in, if the kill signal was received during stage-in.

    :param job: job object.
    :param queue: queue object.
    :param sig: signal.
    :return:
    """

    set_pilot_state(job=job, state="failed")
    error_code = errors.get_kill_signal_error_code(sig)
    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error_code)

    #queue.put(job)
    put_in_queue(job, queue)


def scan_for_jobs(queues):
    """
    Scan queues until at least one queue has a job object. abort if it takes too long time

    :param queues:
    :return: found jobs (list of job objects).
    """

    t0 = time.time()
    found_job = False
    jobs = None

    while time.time() - t0 < 30:
        for q in queues._fields:
            _q = getattr(queues, q)
            jobs = list(_q.queue)
            if len(jobs) > 0:
                logger.info('found %d job(s) in queue %s after %d s - will begin queue monitoring' %
                            (len(jobs), q, time.time() - t0))
                found_job = True
                break
        if found_job:
            break
        else:
            time.sleep(0.1)

    return jobs


def get_queuedata_from_job(queues):
    """
    Return the queuedata object from a job in the given queues object.
    This function is useful if queuedata is needed from a function that does not know about the job object.
    E.g. the pilot monitor does not know about the job object, but still knows
    about the queues from which a job object can be extracted and therefore the queuedata.

    :param queues: queues object.
    :return: queuedata object.
    """

    queuedata = None

    # extract jobs from the queues
    jobs = scan_for_jobs(queues)
    if jobs:
        for job in jobs:
            queuedata = job.infosys.queuedata
            break

    return queuedata


def abort_jobs_in_queues(queues, sig):
    """
    Find all jobs in the queues and abort them.

    :param queues: queues object.
    :param sig: detected kill signal.
    :return:
    """

    jobs_list = []

    # loop over all queues and find all jobs
    for q in queues._fields:
        _q = getattr(queues, q)
        jobs = list(_q.queue)
        for job in jobs:
            if job not in jobs_list:
                jobs_list.append(job)

    logger.info('found %d job(s) in %d queues' % (len(jobs_list), len(queues._fields)))
    for job in jobs_list:
        try:
            logger.info('aborting job %s' % job.jobid)
            declare_failed_by_kill(job, queues.failed_jobs, sig)
        except Exception as e:
            logger.warning('failed to declare job as failed: %s' % e)


def queue_report(queues):
    """

    :param queues:
    :return:
    """

    for q in queues._fields:
        _q = getattr(queues, q)
        jobs = list(_q.queue)
        logger.info('queue %s has %d job(s)' % (q, len(jobs)))


def put_in_queue(obj, queue):
    """
    Put the given object in the given queue.

    :param obj: object.
    :param queue: queue object.
    :return:
    """

    # update job object size (currently not used)
    if isinstance(obj, JobData):
        obj.add_size(obj.get_size())

    queue.put(obj)
