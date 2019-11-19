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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019
# - Wen Guan, wen.guan@cern.ch, 2018

import time
import os
import signal
from subprocess import PIPE

from pilot.common.errorcodes import ErrorCodes
from pilot.control.job import send_state
from pilot.util.auxiliary import get_logger, set_pilot_state
from pilot.util.container import execute
from pilot.util.constants import UTILITY_BEFORE_PAYLOAD, UTILITY_WITH_PAYLOAD, UTILITY_AFTER_PAYLOAD_STARTED, \
    UTILITY_AFTER_PAYLOAD_FINISHED, PILOT_PRE_SETUP, PILOT_POST_SETUP, PILOT_PRE_PAYLOAD, PILOT_POST_PAYLOAD
from pilot.util.timing import add_to_pilot_timing
from pilot.common.exception import PilotException

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


class Executor(object):
    def __init__(self, args, job, out, err, traces):
        self.__args = args
        self.__job = job
        self.__out = out
        self.__err = err
        self.__traces = traces

    def get_job(self):
        """
        Get the job object.
        :return: job object.
        """
        return self.__job

    def setup_payload(self, job, out, err):
        """
        (add description)
        :param job:
        :param out:
        :param err:
        :return:
        """
        # log = get_logger(job.jobid, logger)

        # try:
        # create symbolic link for sqlite200 and geomDB in job dir
        #    for db_name in ['sqlite200', 'geomDB']:
        #         src = '/cvmfs/atlas.cern.ch/repo/sw/database/DBRelease/current/%s' % db_name
        #         link_name = 'job-%s/%s' % (job.jobid, db_name)
        #         os.symlink(src, link_name)
        # except Exception as e:
        #     log.error('could not create symbolic links to database files: %s' % e)
        #     return False

        return True

    def pre_setup(self, job):
        """
        Functions to run pre setup
        :param job: job object
        """
        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_PRE_SETUP, time.time(), self.__args)

    def post_setup(self, job):
        """
        Functions to run post setup
        :param job: job object
        """
        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_POST_SETUP, time.time(), self.__args)

    def utility_before_payload(self, job):
        """
        Functions to run before payload
        :param job: job object
        """
        log = get_logger(job.jobid, logger)

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should we run any additional commands? (e.g. special monitoring commands)
        cmds = user.get_utility_commands_list(order=UTILITY_BEFORE_PAYLOAD)
        if cmds != []:
            for utcmd in cmds:
                log.info('utility command to be executed before the payload: %s' % utcmd)
                # add execution code here
                # store pid in job object job.utilitypids = {<name>: <pid>}
                job.utilitypids[utcmd] = -1

    def utility_with_payload(self, job):
        """
        Functions to run with payload
        :param job: job object
        """
        log = get_logger(job.jobid, logger)

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should any additional commands be prepended to the payload execution string?
        cmds = user.get_utility_commands_list(order=UTILITY_WITH_PAYLOAD)
        if cmds != []:
            for utcmd in cmds:
                log.info('utility command to be executed with the payload: %s' % utcmd)
                # add execution code here

    def utility_after_payload_started(self, job):
        """
        Functions to run after payload started
        :param job: job object
        """
        log = get_logger(job.jobid, logger)

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should any additional commands be executed after the payload?
        cmds = user.get_utility_commands_list(order=UTILITY_AFTER_PAYLOAD_STARTED)
        if cmds != []:
            for utcmd in cmds:
                log.info('utility command to be executed after the payload: %s' % utcmd)

                # how should this command be executed?
                utilitycommand = user.get_utility_command_setup(utcmd, job)
                if not utilitycommand:
                    log.warning('empty utility command - nothing to run')
                    return
                try:
                    proc1 = execute(utilitycommand, workdir=job.workdir, returnproc=True,
                                    usecontainer=False, stdout=PIPE, stderr=PIPE, cwd=job.workdir,
                                    job=job)
                except Exception as e:
                    log.error('could not execute: %s' % e)
                else:
                    # store process handle in job object, and keep track on how many times the command has been launched
                    # also store the full command in case it needs to be restarted later (by the job_monitor() thread)
                    job.utilities[utcmd] = [proc1, 1, utilitycommand]

    def utility_after_payload_finished(self, job):
        """
        Functions to run after payload finished
        :param job: job object
        """
        log = get_logger(job.jobid, logger)

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should any additional commands be prepended to the payload execution string?
        cmds = user.get_utility_commands_list(order=UTILITY_AFTER_PAYLOAD_FINISHED)
        if cmds != []:
            for utcmd in cmds:
                log.info('utility command to be executed with the payload: %s' % utcmd)
                # add execution code here

    def pre_payload(self, job):
        """
        Functions to run pilot pre payload
        :param job: job object
        """
        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_PRE_PAYLOAD, time.time(), self.__args)

    def post_payload(self, job):
        """
        Functions to run pilot post payload
        :param job: job object
        """
        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_POST_PAYLOAD, time.time(), self.__args)

    def run_payload(self, job, out, err):
        """
        (add description)
        :param job: job object
        :param out: (currently not used; deprecated)
        :param err: (currently not used; deprecated)
        :return: proc (subprocess returned by Popen())
        """

        log = get_logger(job.jobid, logger)

        self.pre_setup(job)

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        self.post_setup(job)

        self.utility_before_payload(job)
        self.pre_payload(job)

        self.utility_with_payload(job)

        # for testing looping job:    cmd = user.get_payload_command(job) + ';sleep 240'
        try:
            cmd = user.get_payload_command(job)
        except PilotException as error:
            import traceback
            log.error(traceback.format_exc())
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())
            self.__traces.pilot['error_code'] = job.piloterrorcodes[0]
            log.fatal('could not define payload command (traces error set to: %d)' % self.__traces.pilot['error_code'])
            return None

        log.info("payload execution command: %s" % cmd)

        # replace platform and workdir with new function get_payload_options() or something from experiment specific
        # code
        try:
            proc = execute(cmd, workdir=job.workdir, returnproc=True,
                           usecontainer=True, stdout=out, stderr=err, cwd=job.workdir, job=job)
        except Exception as e:
            log.error('could not execute: %s' % str(e))
            return None

        log.info('started -- pid=%s executable=%s' % (proc.pid, cmd))
        job.pid = proc.pid
        job.pgrp = os.getpgid(job.pid)

        self.utility_after_payload_started(job)

        return proc

    def wait_graceful(self, args, proc, job):
        """
        (add description)
        :param args:
        :param proc:
        :param job:
        :return:
        """

        log = get_logger(job.jobid, logger)

        breaker = False
        exit_code = None
        try:
            iteration = long(0)  # Python 2, do not use 0L since it will create a syntax error in spite of the try
        except Exception:
            iteration = 0  # Python 3, long doesn't exist
        while True:
            time.sleep(0.1)

            iteration += 1
            for i in range(60):  # Python 2/3
                if args.graceful_stop.is_set():
                    breaker = True
                    log.info('breaking -- sending SIGTERM pid=%s' % proc.pid)
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    # proc.terminate()
                    break
                time.sleep(1)
            if breaker:
                log.info('breaking -- sleep 3s before sending SIGKILL pid=%s' % proc.pid)
                time.sleep(3)
                proc.kill()
                break

            exit_code = proc.poll()

            if iteration % 10 == 0:
                log.info('running: iteration=%d pid=%s exit_code=%s' % (iteration, proc.pid, exit_code))
            if exit_code is not None:
                break
            else:
                # send_state(job, args, 'running')
                continue

        return exit_code

    def run(self):
        """
        (add description)
        :return:
        """
        log = get_logger(str(self.__job.jobid), logger)

        exit_code = 1
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()

        if self.setup_payload(self.__job, self.__out, self.__err):
            log.debug('running payload')
            proc = self.run_payload(self.__job, self.__out, self.__err)
            if proc is not None:
                # the process is now running, update the server
                set_pilot_state(job=self.__job, state="running")
                send_state(self.__job, self.__args, self.__job.state)

                log.info('will wait for graceful exit')
                exit_code = self.wait_graceful(self.__args, proc, self.__job)
                state = 'finished' if exit_code == 0 else 'failed'
                set_pilot_state(job=self.__job, state=state)
                log.info('finished pid=%s exit_code=%s state=%s' % (proc.pid, exit_code, self.__job.state))

                if exit_code is None:
                    log.warning('detected unset exit_code from wait_graceful - reset to -1')
                    exit_code = -1

                self.utility_after_payload_finished(self.__job)
                self.post_payload(self.__job)

                # stop any running utilities
                if self.__job.utilities != {}:
                    for utcmd in list(self.__job.utilities.keys()):  # Python 2/3
                        utproc = self.__job.utilities[utcmd][0]
                        if utproc:
                            user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user],
                                              0)  # Python 2/3
                            sig = user.get_utility_command_kill_signal(utcmd)
                            log.info("stopping process \'%s\' with signal %d" % (utcmd, sig))
                            try:
                                os.killpg(os.getpgid(utproc.pid), sig)
                            except Exception as e:
                                log.warning('exception caught: %s (ignoring)' % e)

                            user.post_utility_command_action(utcmd, self.__job)

        return exit_code
