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
from pilot.util.auxiliary import abort_jobs_in_queues
from pilot.util.config import config

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

    traces.pilot['lifetime_start'] = time.time()
    traces.pilot['lifetime_max'] = time.time()

    threadchecktime = int(config.Pilot.thread_check)
    runtime = 0

    try:
        # overall loop counter (ignoring the fact that more than one job may be running)
        n = 0

        while not args.graceful_stop.is_set():
            # every seconds, run the monitoring checks
            if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility
                break

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

            # have we run out of time?
            #if runtime < args.lifetime:
            #    time.sleep(1)
            #    runtime += 1  # note that this is wrong.. use proper time measurement
            #else:
            #    logger.debug('maximum lifetime reached: %s' % args.lifetime)
            #    args.graceful_stop.set()

            n += 1

    except Exception as e:
        print("monitor: exception caught: %s" % e)
        raise PilotException(e)


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
