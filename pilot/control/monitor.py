#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2018

# NOTE: this module should deal with non-job related monitoring, such as thread monitoring. Job monitoring is
#       a task for the job_monitor thread in the Job component.

import logging
import threading
import time

from pilot.common.exception import PilotException, ExceededMaxWaitTime
from pilot.util.auxiliary import abort_jobs_in_queues, get_queuedata_from_job

from pilot.util.config import config
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

    traces.pilot['lifetime_start'] = time.time()  # ie referring to when pilot monitoring begain
    traces.pilot['lifetime_max'] = time.time()

    threadchecktime = int(config.Pilot.thread_check)

    queuedata = get_queuedata_from_job(queues)
    max_running_time = get_max_running_time(args.lifetime, queuedata)

    try:
        # overall loop counter (ignoring the fact that more than one job may be running)
        n = 0

        while not args.graceful_stop.is_set():
            # every seconds, run the monitoring checks
            if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility
                break

            # check if the pilot has run out of time (stop ten minutes before PQ limit)
            time_since_start = get_time_since_start(args)
            grace_time = 10 * 60
            if time_since_start - grace_time > max_running_time:
                logger.fatal('max running time (%d s) minus grace time (%d s) has been exceeded - must abort pilot' %
                             (max_running_time, grace_time))
                logger.info('setting graceful stop')
                args.graceful_stop.set()
                break
            else:
                if n % 60 == 0:
                    logger.info('%d s have passed since pilot start' % time_since_start)
            time.sleep(1)

            # proceed with running the checks
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
        print("monitor: exception caught: %s" % e)
        raise PilotException(e)

    logger.info('monitor control has ended')


#def log_lifetime(sig, frame, traces):
#    logger.info('lifetime: %i used, %i maximum' % (int(time.time() - traces.pilot['lifetime_start']),
#                                                   traces.pilot['lifetime_max']))

def run_checks(queues, args):
    """
    Perform non-job related monitoring checks.

    :param queues:
    :param args:
    :return:
    """

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
                logger.info('will use queuedata.maxtime value for max running time: %d s' % max_running_time)

    return max_running_time
