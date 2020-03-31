#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019


import os
import time

from pilot.common import exception
from pilot.control.payloads import generic
from pilot.eventservice.workexecutor.workexecutor import WorkExecutor
from pilot.util.auxiliary import get_logger
from pilot.util.config import config

import logging
logger = logging.getLogger(__name__)


class Executor(generic.Executor):
    def __init__(self, args, job, out, err, traces):
        super(Executor, self).__init__(args, job, out, err, traces)

    def run_payload(self, job, out, err):
        """
        (add description)

        :param job:
        :param out:
        :param err:
        :return:
        """
        log = get_logger(job.jobid, logger)

        self.pre_setup(job)

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'atlas').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        self.post_setup(job)

        self.utility_before_payload(job)

        self.utility_with_payload(job)

        try:
            executable = user.get_payload_command(job)
        except exception.PilotException as e:
            log.fatal('could not define payload command')
            return None

        log.info("payload execution command: %s" % executable)

        try:
            payload = {'executable': executable, 'workdir': job.workdir, 'output_file': out, 'error_file': err, 'job': job}
            log.debug("payload: %s" % payload)

            log.info("Starting EventService WorkExecutor")
            executor = WorkExecutor(args={'executor_type': config.Payload.executor_type})
            executor.set_payload(payload)
            executor.start()
            log.info("EventService WorkExecutor started")

            log.info("ESProcess started with pid: %s" % executor.get_pid())
            job.pid = executor.get_pid()
            if job.pid:
                job.pgrp = os.getpgid(job.pid)

            self.utility_after_payload_started(job)
        except Exception as e:
            log.error('could not execute: %s' % str(e))
            return None

        return executor

    def wait_graceful(self, args, proc, job):
        """
        (add description)

        :param args:
        :param proc:
        :param job:
        :return:
        """

        log = get_logger(job.jobid, logger)

        t1 = time.time()
        while proc.is_alive():
            if args.graceful_stop.is_set():
                log.debug("Graceful stop is set, stopping work executor")
                proc.stop()
                break
            if time.time() > t1 + 300:  # 5 minutes
                log.info("Process is still running")
                t1 = time.time()
            time.sleep(2)

        while proc.is_alive():
            time.sleep(2)
        exit_code = proc.get_exit_code()
        return exit_code
