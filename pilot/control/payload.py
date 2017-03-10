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
import subprocess
import threading
import time

from pilot.util import information

import logging
logger = logging.getLogger(__name__)


def control(queues, graceful_stop, traces, args):

    threads = [threading.Thread(target=validate_pre,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=execute,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=validate_post,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args})]

    batchqueues = information.get_queues()

    if args.queue not in [queue['name'] for queue in batchqueues]:
        logger.critical('configured queue not found: {0} -- aborting'.format(args.queue))
        graceful_stop.set()
        return

    if [queue for queue in batchqueues if queue['name'] == args.queue and queue['state'] == 'ACTIVE'] == []:
        logger.critical('configured queue is NOT ACTIVE: {0} -- aborting'.format(args.queue))
        graceful_stop.set()
        return

    logger.info('configured queue: {0}'.format(args.queue))

    [t.start() for t in threads]


def validate_pre(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        if __validate_payload(job):
            queues.validated_payloads.put(job)
        else:
            queues.failed_payloads.put(job)


def __validate_payload(job):
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('payload did not validate correctly -- skipping')
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'payload failed random validation'
    #     return False
    return True


def execute(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.validated_payloads.get(block=True, timeout=1)

            q_snapshot = list(queues.finished_data_in.queue)
            peek = [s_job['realDatasetsIn'] for s_job in q_snapshot if job['PandaID'] == s_job['PandaID']]
            if set(peek) != set(job['realDatasetsIn'].split(',')):
                logger.debug('{0}: nr_files_done={1} needed={2}'.format(job['PandaID'], len(peek), len(job['realDatasetsIn'].split(','))))
                queues.validated_payloads.put(job)
                for i in xrange(10):
                    if graceful_stop.is_set():
                            break
                    time.sleep(0.1)
                continue

            logger.debug('{0}: set job state=starting'.format(job['PandaID']))
            cmd = 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates --cert' \
                  ' $X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY "https://pandaserver.cern.ch:25443/server/panda/updateJob?jobId=' \
                  '{0}&state=starting"'.format(job['PandaID'])
            logger.debug('executing: {0}'.format(cmd))
            s, o = commands.getstatusoutput(cmd)
            if s != 0:
                logger.warning('{0}: set job state=starting failed: {1}'.format(job['PandaID'], o))
            else:
                logger.info('{0}: confirmed job state=starting'.format(job['PandaID']))

            executable = ['/usr/bin/env', job['transformation']] + job['jobPars'].split()
            logger.debug('{0}: executable={1}'.format(job['PandaID'], executable))

            logger.debug('{0}: opening payload stdout/err logs'.format(job['PandaID']))
            payload_stdout = open('job-{0}/payload.stdout'.format(job['PandaID']), 'wb')
            payload_stderr = open('job-{0}/payload.stderr'.format(job['PandaID']), 'wb')

            try:
                process = subprocess.Popen(executable,
                                           bufsize=-1,
                                           stdout=payload_stdout,
                                           stderr=payload_stderr,
                                           cwd='job-{0}'.format(job['PandaID']))
            except Exception as e:
                logger.error('{0}: could not execute: {1}'.format(job['PandaID'], e))
                queues.failed_payloads.put(job)
                continue

            logger.info('{0}: started -- pid={1} executable={2}'.format(job['PandaID'], process.pid, executable))

            breaker = False
            while True:
                for i in xrange(100):
                    if graceful_stop.is_set():
                        breaker = True
                        logger.debug('{0}: breaking -- sending SIGTERM pid={1}'.format(job['PandaID'], process.pid))
                        process.terminate()
                        break
                    time.sleep(0.1)
                if breaker:
                    logger.debug('{0}: breaking -- sleep 3s before sending SIGKILL pid={1}'.format(job['PandaID'], process.pid))
                    time.sleep(3)
                    process.kill()
                    break

                exit_code = process.poll()
                logger.info('{0}: running: pid={1} exit_code={2}'.format(job['PandaID'], process.pid, exit_code))
                if exit_code is not None:
                    break
                else:
                    logger.debug('{0}: set job state=running'.format(job['PandaID']))
                    cmd = 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates' \
                          ' --cert $X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY' \
                          ' "https://pandaserver.cern.ch:25443/server/panda/updateJob?jobId={0}&state=running"'.format(job['PandaID'])
                    logger.debug('executing: {0}'.format(cmd))
                    s, o = commands.getstatusoutput(cmd)
                    if s != 0:
                        logger.warning('{0}: set job state=running failed: {1}'.format(job['PandaID'], o))
                    else:
                        logger.info('{0}: confirmed job state=running'.format(job['PandaID']))
                    continue

            logger.debug('{0}: closing payload stdout/err logs'.format(job['PandaID']))
            payload_stdout.close()
            payload_stderr.close()
            logger.info('{0}: finished pid={0} exit_code={1}'.format(job['PandaID'], process.pid, exit_code))

            if exit_code == 0:
                queues.finished_payloads.put(job)
            else:
                queues.failed_payloads.put(job)

        except Queue.Empty:
            continue


def validate_post(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.finished_payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        logger.debug('{0}: adding job report for stageout'.format(job['PandaID']))
        with open('job-{0}/jobReport.json'.format(job['PandaID'])) as data_file:
            job['jobReport'] = json.load(data_file)

        queues.data_out.put(job)
