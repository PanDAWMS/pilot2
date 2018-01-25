#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import Queue
import json
import os
import subprocess
import threading
import time

from pilot.control.job import send_state
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def control(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    threads = [threading.Thread(target=validate_pre,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=execute_payloads,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=validate_post,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=failed_post,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args})]
    [t.start() for t in threads]


def validate_pre(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """
    while not args.graceful_stop.is_set():
        try:
            job = queues.payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        if _validate_payload(job):
            queues.validated_payloads.put(job)
        else:
            queues.failed_payloads.put(job)


def _validate_payload(job):
    """
    (add description)

    :param job:
    :return:
    """
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('payload did not validate correctly -- skipping')
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'payload failed random validation'
    #     return False
    return True


def setup_payload(job, out, err):
    """
    (add description)

    :param job:
    :param out:
    :param err:
    :return:
    """
    # log = logger.getChild(str(job['PandaID']))

    # try:
    # create symbolic link for sqlite200 and geomDB in job dir
    #    for db_name in ['sqlite200', 'geomDB']:
    #         src = '/cvmfs/atlas.cern.ch/repo/sw/database/DBRelease/current/%s' % db_name
    #         link_name = 'job-%s/%s' % (job['PandaID'], db_name)
    #         os.symlink(src, link_name)
    # except Exception as e:
    #     log.error('could not create symbolic links to database files: %s' % e)
    #     return False

    return True


def run_payload(job, out, err):
    """
    (add description)

    :param job: job object
    :param out: (currently not used; deprecated)
    :param err: (currently not used; deprecated)
    :return: proc (subprocess returned by Popen())
    """
    log = logger.getChild(str(job['PandaID']))

    # get the payload command from the user specific code
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], -1)
    cmd = user.get_payload_command(job)
    log.info("payload execution command: %s" % cmd)

    # replace platform and workdir with new function get_payload_options() or someting from experiment specific code
    try:
        # proc = subprocess.Popen(cmd,
        #                         bufsize=-1,
        #                         stdout=out,
        #                         stderr=err,
        #                         cwd=job['working_dir'],
        #                         shell=True)

        proc = execute(cmd, platform=job['cmtConfig'], workdir=job['working_dir'], returnproc=True,
                       usecontainer=True, stdout=out, stderr=err, cwd=job['working_dir'])
    except Exception as e:
        log.error('could not execute: %s' % str(e))
        return None

    log.info('started -- pid=%s executable=%s' % (proc.pid, cmd))

    return proc


def wait_graceful(args, proc, job):
    """
    (add description)

    :param args:
    :param proc:
    :param job:
    :return:
    """
    log = logger.getChild(str(job['PandaID']))

    breaker = False
    exit_code = None
    while True:
        for i in xrange(100):
            if args.graceful_stop.is_set():
                breaker = True
                log.debug('breaking -- sending SIGTERM pid=%s' % proc.pid)
                proc.terminate()
                break
            time.sleep(0.1)
        if breaker:
            log.debug('breaking -- sleep 3s before sending SIGKILL pid=%s' % proc.pid)
            time.sleep(3)
            proc.kill()
            break

        exit_code = proc.poll()
        log.info('running: pid=%s exit_code=%s' % (proc.pid, exit_code))
        if exit_code is not None:
            break
        else:
            # send_state(job, args, 'running')
            continue

    return exit_code


def execute_payloads(queues, traces, args):
    """
    Execute queued payloads.

    :param queues:
    :param traces:
    :param args:
    :return:
    """
    while not args.graceful_stop.is_set():
        try:
            job = queues.validated_payloads.get(block=True, timeout=1)
            log = logger.getChild(str(job['PandaID']))

            q_snapshot = list(queues.finished_data_in.queue)
            peek = [s_job for s_job in q_snapshot if job['PandaID'] == s_job['PandaID']]
            if len(peek) == 0:
                queues.validated_payloads.put(job)
                for i in xrange(10):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(0.1)
                continue

            log.debug('opening payload stdout/err logs')
            out = open(os.path.join(job['working_dir'], 'payload.stdout'), 'wb')
            err = open(os.path.join(job['working_dir'], 'payload.stderr'), 'wb')

            log.debug('setting up payload environment')
            send_state(job, args, 'starting')

            exit_code = 1
            if setup_payload(job, out, err):
                log.debug('running payload')
                send_state(job, args, 'running')
                proc = run_payload(job, out, err)
                if proc is not None:
                    exit_code = wait_graceful(args, proc, job)
                    log.info('finished pid=%s exit_code=%s' % (proc.pid, exit_code))

            log.debug('closing payload stdout/err logs')
            out.close()
            err.close()

            if exit_code == 0:
                job['transExitCode'] = 0
                queues.finished_payloads.put(job)
            else:
                job['transExitCode'] = exit_code
                queues.failed_payloads.put(job)

        except Queue.Empty:
            continue


def validate_post(queues, traces, args):
    """
    Validate finished payloads.
    If payload finished correctly, add the job to the data_out queue. If it failed, add it to the data_out queue as
    well but only for log stage-out.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        # finished payloads
        try:
            job = queues.finished_payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(str(job['PandaID']))

        # note: all PanDA users should generate a job report json file (required by Harvester)
        log.debug('adding job report for stageout')
        stageout = "all"
        with open(os.path.join(job['working_dir'], config.Payload.jobreport)) as data_file:
            job['job_report'] = json.load(data_file)

            # extract info from job report
            # === experiment specific ===
            if 'exeErrorCode' in job['job_report']:
                job['exeErrorCode'] = job['job_report']['exeErrorCode']
                if job['exeErrorCode'] == 0:
                    stageout = "all"
                else:
                    log.info('payload failed: exeErrorCode=%d' % job['exeErrorCode'])
                    stageout = "log"
            if 'exeErrorDiag' in job['job_report']:
                job['exeErrorDiag'] = job['job_report']['exeErrorDiag']
                if job['exeErrorDiag'] != "":
                    log.warning('payload failed: exeErrorDiag=%s' % job['exeErrorDiag'])

        job['stageout'] = stageout  # output and log file or only log file
        queues.data_out.put(job)


def failed_post(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        # finished payloads
        try:
            job = queues.failed_payloads.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(str(job['PandaID']))

        log.debug('adding log for log stageout')

        job['stageout'] = "log"  # only stage-out log file
        queues.data_out.put(job)
# wrong queue??