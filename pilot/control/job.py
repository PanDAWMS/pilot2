#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017

import Queue
import os
import threading
import time

from pilot.util import information
from pilot.util import https
from pilot.util.job_description_fixer import description_fixer

import logging
logger = logging.getLogger(__name__)


def control(queues, graceful_stop, traces, args):

    threads = [threading.Thread(target=validate,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=retrieve,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=create_data_payload,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args})]

    sites = information.get_sites()

    if args.site not in [s['name'] for s in sites]:
        logger.critical('configured site not found: {0} -- aborting'.format(args.site))
        graceful_stop.set()
        return

    if not [site for site in sites if site['name'] == args.site and site['state'] == 'ACTIVE']:
        logger.critical('configured site is NOT ACTIVE: {0} -- aborting'.format(args.site))
        graceful_stop.set()
        return

    logger.info('configured site: {0}'.format(args.site))

    [t.start() for t in threads]


def _validate_job(job):
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('{0}: job did not validate correctly -- skipping'.format(job['job_id']))
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'job failed random validation'
    #     return False
    return True


def send_state(job, state, xml=None):
    log = logger.getChild(job['job_id'])
    log.debug('set job state={0}'.format(state))

    data = {
        'jobId': job['job_id'],
        'state': state
    }

    if xml is not None:
        data['xml'] = xml

    try:
        if https.request('https://pandaserver.cern.ch:25443/server/panda/updateJob', data=data) is not None:
            log.info('confirmed job state={0}'.format(state))
            return True
    except Exception as e:
        log.warning('while setting job state, Exception caught: ' + str(e.message))
        pass

    log.warning('set job state={0} failed'.format(state))
    return False


def validate(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.jobs.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(job['job_id'])

        traces.pilot['nr_jobs'] += 1

        if _validate_job(job):

            log.debug('creating job working directory')
            job_dir = 'job-{0}'.format(job['job_id'])
            try:
                os.mkdir(job_dir)
                job['working_dir'] = job_dir
            except Exception as e:
                log.debug('cannot create job working directory: {0}'.format(str(e)))
                queues.failed_jobs.put(job)
                break

            log.debug('symlinking pilot log')
            try:
                os.symlink('../pilotlog.txt', os.path.join(job_dir, 'pilotlog.txt'))
            except Exception as e:
                log.debug('cannot symlink pilot log: {0}'.format(str(e)))
                queues.failed_jobs.put(job)
                break

            queues.validated_jobs.put(job)
        else:
            queues.failed_jobs.put(job)


def create_data_payload(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.validated_jobs.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        queues.data_in.put(job)
        queues.payloads.put(job)


def retrieve(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():

        logger.debug('trying to fetch job')

        data = {
            'getProxyKey': False,  # do we need it?
            'computingElement': args.queue,
            'siteName': args.resource,
            'workingGroup': '',  # do we need it?
            'prodSourceLabel': 'mtest'
        }

        res = description_fixer(https.request('https://pandaserver.cern.ch:25443/server/panda/getJob', data=data))

        if res['status_code'] != 0:
            logger.warning('did not get a job -- sleep 1000s and repeat -- status: {0}'.format(res['status_code']))
            for i in xrange(10000):
                if graceful_stop.is_set():
                    break
                time.sleep(0.1)
        else:
            logger.info('got job: {0}'.format(res['job_id']))
            queues.jobs.put(res)
