#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import functools
import signal
from collections import namedtuple

# from pilot.util.harvester import get_initial_work_report, publish_work_report
# from pilot.util.auxiliary import time_stamp
# from pilot.util.filehandling import tar_files, remove, remove_empty_directories

from pilot.util.constants import SUCCESS, FAILURE

import logging
logger = logging.getLogger(__name__)


def interrupt(args, signum, frame):
    logger.info('caught signal: %s' % [v for v, k in signal.__dict__.iteritems() if k == signum][0])
    args.graceful_stop.set()


def run(args):
    """
    Main execution function for the generic HPC workflow.

    :param args: pilot arguments.
    :returns: traces object.
    """

    try:
        logger.info('setting up signal handling')
        signal.signal(signal.SIGINT, functools.partial(interrupt, args))

        logger.info('setting up tracing')
        traces = namedtuple('traces', ['pilot'])
        traces.pilot = {'state': SUCCESS,
                        'nr_jobs': 0}

        if args.hpc_resource == '':
            logger.critical('hpc resource not specified, cannot continue')
            traces.pilot['state'] = FAILURE
            return traces

        # get the resource reference
        resource = __import__('pilot.resource.%s' % args.hpc_resource, globals(), locals(), [args.hpc_resource], -1)

        # example usage:
        logger.info('setup for resource %s: %s' % (args.hpc_resource, str(resource.get_setup())))

        # extract user specific info from job report
        user = __import__('pilot.user.%s.common' % args.pilot_user.lower(), globals(), locals(),
                          [args.pilot_user.lower()], -1)
        # example usage:
        # user.remove_redundant_files()
        # user.cleanup_payload()
        # parse_jobreport_data()

        # implement main function here
        # ..
    except Exception as e:
        logger.fatal('exception caught: %s' % e)

    return traces
