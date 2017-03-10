#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017

import Queue
import commands
import json
import os
import socket
import threading
import time

from pilot.util import information

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
    #     logger.warning('{0}: job did not validate correctly -- skipping'.format(job['PandaID']))
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'job failed random validation'
    #     return False
    return True


def validate(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.jobs.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        traces.pilot['nr_jobs'] += 1

        if _validate_job(job):

            logger.debug('{0}: creating job working directory'.format(job['PandaID']))
            try:
                os.mkdir('job-{0}'.format(job['PandaID']))
            except Exception as e:
                logger.debug('{0}: cannot create job working directory: {1}'.format(job['PandaID'], str(e)))
                queues.failed_jobs.put(job)
                break

            logger.debug('{0}: symlinking pilot log'.format(job['PandaID']))
            try:
                os.symlink('../pilotlog.txt', 'job-{0}/pilotlog.txt'.format(job['PandaID']))
            except Exception as e:
                logger.debug('{0}: cannot symlink pilot log: {1}'.format(job['PandaID'], str(e)))
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

        cmd = 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates --cert' \
              ' $X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY "https://pandaserver.cern.ch:25443/server/panda/getJob?node=' \
              '{0}&siteName={1}&computingElement={2}&prodSourceLabel=mtest"'.format(socket.getfqdn(), args.resource, args.queue)
        logger.debug('executing: {0}'.format(cmd))
        s, o = commands.getstatusoutput(cmd)

        if s != 0:
            logger.warning('could not establish connection to get a job -- sleep 1s and repeat -- (exit: {0}): {1}'.format(s, o))
            for i in xrange(10):
                if graceful_stop.is_set():
                    break
                time.sleep(0.1)
        else:
            res = json.loads(o)
            if res['StatusCode'] != 0:
                logger.warning('did not get a job -- sleep 1000s and repeat -- status: {0}'.format(res['StatusCode']))
                for i in xrange(10000):
                    if graceful_stop.is_set():
                        break
                    time.sleep(0.1)
            else:
                logger.info('got job: {0}'.format(res['PandaID']))
                queues.jobs.put(res)
