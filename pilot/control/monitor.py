#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019

# NOTE: this module should deal with non-job related monitoring, such as thread monitoring. Job monitoring is
#       a task for the job_monitor thread in the Job component.

import logging
import threading
import time
import re
from os import environ, getpid, getuid
from subprocess import Popen, PIPE

from pilot.common.exception import PilotException, ExceededMaxWaitTime
from pilot.util.auxiliary import check_for_final_server_update
from pilot.util.config import config
from pilot.util.constants import MAX_KILL_WAIT_TIME
# from pilot.util.container import execute
from pilot.util.queuehandling import get_queuedata_from_job, abort_jobs_in_queues
from pilot.util.timing import get_time_since_start

logger = logging.getLogger(__name__)


# Monitoring of threads functions

def control(queues, traces, args):
    """
    Main control function, run from the relevant workflow module.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    t0 = time.time()
    traces.pilot['lifetime_start'] = t0  # ie referring to when pilot monitoring began
    traces.pilot['lifetime_max'] = t0

    threadchecktime = int(config.Pilot.thread_check)

    # for CPU usage debugging
    cpuchecktime = int(config.Pilot.cpu_check)
    tcpu = t0

    queuedata = get_queuedata_from_job(queues)
    max_running_time = get_max_running_time(args.lifetime, queuedata)

    try:
        # overall loop counter (ignoring the fact that more than one job may be running)
        n = 0

        while not args.graceful_stop.is_set():
            # every seconds, run the monitoring checks
            if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility
                logger.warning('aborting monitor loop since graceful_stop has been set')
                break

            # abort if kill signal arrived too long time ago, ie loop is stuck
            if args.kill_time and int(time.time()) - args.kill_time > MAX_KILL_WAIT_TIME:
                logger.warning('loop has run for too long time - will abort')
                args.graceful_stop.set()
                break

            # check if the pilot has run out of time (stop ten minutes before PQ limit)
            time_since_start = get_time_since_start(args)
            grace_time = 10 * 60
            if time_since_start - grace_time > max_running_time:
                logger.fatal('max running time (%d s) minus grace time (%d s) has been exceeded - must abort pilot' %
                             (max_running_time, grace_time))
                logger.info('setting REACHED_MAXTIME and graceful stop')
                environ['REACHED_MAXTIME'] = 'REACHED_MAXTIME'  # TODO: use singleton instead
                # do not set graceful stop if pilot has not finished sending the final job update
                # i.e. wait until SERVER_UPDATE is FINAL_DONE
                check_for_final_server_update(args.update_server)
                args.graceful_stop.set()
                break
            else:
                if n % 60 == 0:
                    logger.info('%d s have passed since pilot start' % time_since_start)
            time.sleep(1)

            # time to check the CPU?
            if int(time.time() - tcpu) > cpuchecktime and False:  # for testing only
                processes = get_process_info('python pilot2/pilot.py', pid=getpid())
                if processes:
                    logger.info('-' * 100)
                    logger.info('PID=%d has CPU usage=%s%% MEM usage=%s%% CMD=%s' % (getpid(), processes[0], processes[1], processes[2]))
                    n = processes[3]
                    if n > 1:
                        logger.info('there are %d such processes running' % n)
                    else:
                        logger.info('there is %d such process running' % n)
                    logger.info('-' * 100)
                tcpu = time.time()

            # proceed with running the other checks
            run_checks(queues, args)

            # thread monitoring
            if int(time.time() - traces.pilot['lifetime_start']) % threadchecktime == 0:
                # get all threads
                for thread in threading.enumerate():
                    # logger.info('thread name: %s' % thread.name)
                    if not thread.is_alive():
                        logger.fatal('thread \'%s\' is not alive' % thread.name)
                        # args.graceful_stop.set()

            n += 1

    except Exception as e:
        print(("monitor: exception caught: %s" % e))
        raise PilotException(e)

    logger.info('[monitor] control thread has ended')

#def log_lifetime(sig, frame, traces):
#    logger.info('lifetime: %i used, %i maximum' % (int(time.time() - traces.pilot['lifetime_start']),
#                                                   traces.pilot['lifetime_max']))


def get_process_info(cmd, user=None, args='aufx', pid=None):
    """
    Return process info for given command.
    The function returns a list with format [cpu, mem, command, number of commands] as returned by 'ps -u user args' for
    a given command (e.g. python pilot2/pilot.py).

    Example
      get_processes_for_command('sshd:')

      nilspal   1362  0.0  0.0 183424  2528 ?        S    12:39   0:00 sshd: nilspal@pts/28
      nilspal   1363  0.0  0.0 136628  2640 pts/28   Ss   12:39   0:00  _ -tcsh
      nilspal   8603  0.0  0.0  34692  5072 pts/28   S+   12:44   0:00      _ python monitor.py
      nilspal   8604  0.0  0.0  62036  1776 pts/28   R+   12:44   0:00          _ ps -u nilspal aufx --no-headers

      -> ['0.0', '0.0', 'sshd: nilspal@pts/28', 1]

    :param cmd: command (string).
    :param user: user (string).
    :param args: ps arguments (string).
    :param pid: process id (int).
    :return: list with process info (l[0]=cpu usage(%), l[1]=mem usage(%), l[2]=command(string)).
    """

    processes = []
    n = 0
    if not user:
        user = getuid()
    pattern = re.compile(r"\S+|[-+]?\d*\.\d+|\d+")
    arguments = ['ps', '-u', user, args, '--no-headers']

    process = Popen(arguments, stdout=PIPE, stderr=PIPE)
    stdout, notused = process.communicate()
    for line in stdout.splitlines():
        found = re.findall(pattern, line)
        if found is not None:
            processid = found[1]
            cpu = found[2]
            mem = found[3]
            command = ' '.join(found[10:])
            if cmd in command:
                n += 1
                if processid == str(pid):
                    processes = [cpu, mem, command]

    if processes:
        processes.append(n)

    return processes


def run_checks(queues, args):
    """
    Perform non-job related monitoring checks.

    :param queues:
    :param args:
    :return:
    """

    # check CPU consumption of pilot process and its children

    if args.abort_job.is_set():
        # find all running jobs and stop them, find all jobs in queues relevant to this module
        abort_jobs_in_queues(queues, args.signal)

        t_max = 2 * 60
        logger.warning('pilot monitor received instruction that abort_job has been requested')
        logger.warning('will wait for a maximum of %d seconds for threads to finish' % t_max)
        t0 = time.time()
        while time.time() - t0 < t_max:
            if args.job_aborted.is_set():
                logger.warning('job_aborted has been set - aborting pilot monitoring')
                args.abort_job.clear()
                return
            time.sleep(1)

        if args.graceful_stop.is_set():
            logger.info('graceful_stop already set')
        else:
            logger.warning('setting graceful_stop')
            args.graceful_stop.set()

        if not args.job_aborted.is_set():
            logger.warning('will wait for a maximum of %d seconds for graceful_stop to take effect' % t_max)
            t_max = 10
            t0 = time.time()
            while time.time() - t0 < t_max:
                if args.job_aborted.is_set():
                    logger.warning('job_aborted has been set - aborting pilot monitoring')
                    args.abort_job.clear()
                    return
                time.sleep(1)

            diagnostics = 'reached maximum waiting time - threads should have finished'
            args.abort_job.clear()
            args.job_aborted.set()
            raise ExceededMaxWaitTime(diagnostics)


def get_max_running_time(lifetime, queuedata):
    """
    Return the maximum allowed running time for the pilot.
    The max time is set either as a pilot option or via the schedconfig.maxtime for the PQ in question.

    :param lifetime: optional pilot option time in seconds (int).
    :param queuedata: queuedata object
    :return: max running time in seconds (int).
    """

    #return 40

    max_running_time = lifetime

    # use the schedconfig value if set, otherwise use the pilot option lifetime value
    if not queuedata:
        logger.warning('queuedata could not be extracted from queues, will use default for max running time '
                       '(%d s)' % max_running_time)
    else:
        if queuedata.maxtime:
            try:
                max_running_time = int(queuedata.maxtime)
            except Exception as e:
                logger.warning('exception caught: %s' % e)
                logger.warning('failed to convert maxtime from queuedata, will use default value for max running time '
                               '(%d s)' % max_running_time)
            else:
                if max_running_time == 0:
                    max_running_time = lifetime  # fallback to default value
                    logger.info('will use default value for max running time: %d s' % max_running_time)
                else:
                    logger.info('will use queuedata.maxtime value for max running time: %d s' % max_running_time)

    return max_running_time
