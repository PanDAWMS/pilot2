#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# This module contains implementations of job monitoring tasks

import os
import time
from subprocess import PIPE

from pilot.common.errorcodes import ErrorCodes
from pilot.util.config import config
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def job_monitor_tasks(job, mt):
    """
    Perform the tasks for the job monitoring.

    :param job: job object.
    :param mt: `MonitoringTime` object.
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    log = logger.getChild(job.jobid)
    current_time = int(time.time())

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    userproxy = __import__('pilot.user.%s.proxy' % pilot_user, globals(), locals(), [pilot_user], -1)

    log.info('ct=%d' % current_time)
    log.info('ct_proxy=%s' % mt.get('ct_proxy'))
    log.info('pvt=%d' % config.Pilot.proxy_verification_time)
    # is it time to verify the proxy?
    if current_time - mt.get('ct_proxy') > config.Pilot.proxy_verification_time:
        # is the proxy still valid?
        exit_code, diagnostics = userproxy.verify_proxy()
        if exit_code != 0:
            return exit_code, diagnostics
        else:
            # update the ct_proxy with the current time
            mt.update('ct_proxy')

    # is it time to check for looping jobs?
    looping_limit = get_looping_job_limit(job)
    if current_time - mt.get('ct_looping') > config.Pilot.looping_verifiction_time:
        # is the job looping?
        #exit_code, diagnostics = loopingjob.
        pass

    # is the job using too much space?

    # Is the payload stdout within allowed limits?

    # Are the output files within allowed limits?

    # make sure that any utility commands are still running
    if job.utilities != {}:
        job = utility_monitor(job)

    # send heartbeat

    return exit_code, diagnostics


def utility_monitor(job):
    """
    Make sure that any utility commands are still running.
    In case a utility tool has crashed, this function may restart the process.
    The function is used by the job monitor thread.

    :param job: job object.
    :return: updated job object.
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    usercommon = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], -1)

    log = logger.getChild(job.jobid)

    # loop over all utilities
    for utcmd in job.utilities.keys():

        # make sure the subprocess is still running
        utproc = job.utilities[utcmd][0]
        if not utproc.poll() is None:
            # if poll() returns anything but None it means that the subprocess has ended - which it
            # should not have done by itself
            utility_subprocess_launches = job.utilities[utcmd][1]
            if utility_subprocess_launches <= 5:
                log.warning('dectected crashed utility subprocess - will restart it')
                utility_command = job.utilities[utcmd][2]

                try:
                    proc1 = execute(utility_command, workdir=job.workdir, returnproc=True, usecontainer=True,
                                    stdout=PIPE, stderr=PIPE, cwd=job.workdir, queuedata=job.infosys.queuedata)
                except Exception as e:
                    log.error('could not execute: %s' % e)
                else:
                    # store process handle in job object, and keep track on how many times the
                    # command has been launched
                    job.utilities[utcmd] = [proc1, utility_subprocess_launches + 1, utility_command]
            else:
                log.warning('dectected crashed utility subprocess - too many restarts, will not restart %s again' %
                            utcmd)
        else:
            # log.info('utility %s is still running' % utcmd)

            # check the utility output (the selector option adds a substring to the output file name)
            filename = usercommon.get_utility_command_output_filename(utcmd, selector=True)
            path = os.path.join(job.workdir, filename)
            if os.path.exists(path):
                log.info('file: %s exists' % path)
            else:
                log.warning('file: %s does not exist' % path)
    return job


def get_looping_job_limit(job):
    """
    Get the time limit for looping job detection.

    :param job: job object.
    :return: looping job time limit (int).
    """

    try:
        looping_limit = int(config.Pilot.looping_limit_default_prod)
        if job.is_analysis():
            looping_limit = int(config.Pilot.looping_limit_default_user)
    except ValueError as e:
        looping_limit = 12 * 3600
        logger.warning('exception caught: %s (using default looping limit: %d s)' % (e, looping_limit))

    return looping_limit
