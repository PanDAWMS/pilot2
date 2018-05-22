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
from pilot.util.processes import get_instant_cpu_consumption_time
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.parameters import convert_to_int
from loopingjob import looping_job

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def job_monitor_tasks(job, mt, verify_proxy):
    """
    Perform the tasks for the job monitoring.

    :param job: job object.
    :param mt: `MonitoringTime` object.
    :param verify_proxy: True if the proxy should be verified. False otherwise.
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    log = logger.getChild(job.jobid)
    current_time = int(time.time())

    # update timing info
    job.cpuconsumptiontime = get_instant_cpu_consumption_time(job.pid)
    job.cpuconsumptionunit = "s"
    job.cpuconversionfactor = 1.0
    log.info('current CPU consumption time: %s' % job.cpuconsumptiontime)

    # should the proxy be verified?
    if verify_proxy:
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        userproxy = __import__('pilot.user.%s.proxy' % pilot_user, globals(), locals(), [pilot_user], -1)

        # is it time to verify the proxy?
        proxy_verification_time = convert_to_int(config.Pilot.proxy_verification_time)
        if current_time - mt.get('ct_proxy') > proxy_verification_time:
            # is the proxy still valid?
            exit_code, diagnostics = userproxy.verify_proxy()
            if exit_code != 0:
                return exit_code, diagnostics
            else:
                # update the ct_proxy with the current time
                mt.update('ct_proxy')

    # is it time to check for looping jobs?
    log.info('current_time - mt.get(ct_looping) = %d' % (current_time - mt.get('ct_looping')))
    log.info('config.Pilot.looping_verifiction_time = %s' % config.Pilot.looping_verifiction_time)
    looping_verifiction_time = convert_to_int(config.Pilot.looping_verifiction_time)
    if current_time - mt.get('ct_looping') > looping_verifiction_time:
        # is the job looping?
        try:
            exit_code, diagnostics = looping_job(job, mt)
        except Exception as e:
            log.warning('exeption caught in looping job algrithm: %s' % e)
        else:
            if exit_code != 0:
                return exit_code, diagnostics
            else:
                # update the ct_proxy with the current time
                mt.update('ct_looping')

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
