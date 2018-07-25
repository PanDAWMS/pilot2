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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-8
# - Wen Guan, wen.guan@cern.ch, 2018

import time
import os
import signal
from subprocess import PIPE

from pilot.control.job import send_state
from pilot.util.auxiliary import get_logger
from pilot.util.container import execute
from pilot.util.constants import UTILITY_BEFORE_PAYLOAD, UTILITY_WITH_PAYLOAD, UTILITY_AFTER_PAYLOAD, \
    PILOT_PRE_SETUP, PILOT_POST_SETUP, PILOT_PRE_PAYLOAD, PILOT_POST_PAYLOAD
from pilot.util.timing import add_to_pilot_timing
from pilot.common.exception import PilotException

import logging
logger = logging.getLogger(__name__)


class Executor(object):
    def __init__(self, args, job, out, err):
        self.__args = args
        self.__job = job
        self.__out = out
        self.__err = err

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
        # log = get_logger(job.jobid)

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

    def run_payload(self, job, out, err):
        """
        (add description)

        :param job: job object
        :param out: (currently not used; deprecated)
        :param err: (currently not used; deprecated)
        :return: proc (subprocess returned by Popen())
        """

        log = get_logger(job.jobid)

        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_PRE_SETUP, time.time())

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], -1)
        # for testing looping job:    cmd = user.get_payload_command(job) + ';sleep 240'
        try:
            cmd = user.get_payload_command(job)
            #cmd = user.get_payload_command(job) + ';sleep 240'
        except PilotException as e:
            log.fatal('could not define payload command')
            return None

        log.info("payload execution command: %s" % cmd)

        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_POST_SETUP, time.time())

        # should we run any additional commands? (e.g. special monitoring commands)
        cmds = user.get_utility_commands_list(order=UTILITY_BEFORE_PAYLOAD)
        if cmds != []:
            for utcmd in cmds:
                log.info('utility command to be executed before the payload: %s' % utcmd)
                # add execution code here
                # store pid in job object job.utilitypids = {<name>: <pid>}
                job.utilitypids[utcmd] = -1

        # should any additional commands be prepended to the payload execution string?
        cmds = user.get_utility_commands_list(order=UTILITY_WITH_PAYLOAD)
        if cmds != []:
            for utcmd in cmds:
                log.info('utility command to be executed with the payload: %s' % utcmd)
                # add execution code here

        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_PRE_PAYLOAD, time.time())

        # replace platform and workdir with new function get_payload_options() or something from experiment specific
        # code
        try:
            proc = execute(cmd, workdir=job.workdir, returnproc=True,
                           usecontainer=False, stdout=out, stderr=err, cwd=job.workdir, job=job)
        except Exception as e:
            log.error('could not execute: %s' % str(e))
            return None

        log.info('started -- pid=%s executable=%s' % (proc.pid, cmd))
        job.pid = proc.pid

        # WRONG PLACE FOR THE FOLLOWING? MAIN PAYLOAD IS STILL RUNNING!!!

        # should any additional commands be executed after the payload?
        cmds = user.get_utility_commands_list(order=UTILITY_AFTER_PAYLOAD)
        if cmds != []:
            for utcmd in cmds:
                log.info('utility command to be executed after the payload: %s' % utcmd)

                # how should this command be executed?
                utilitycommand = user.get_utility_command_setup(utcmd, job)

                try:
                    proc1 = execute(utilitycommand, workdir=job.workdir, returnproc=True,
                                    usecontainer=True, stdout=PIPE, stderr=PIPE, cwd=job.workdir,
                                    job=job)
                except Exception as e:
                    log.error('could not execute: %s' % e)
                else:
                    # store process handle in job object, and keep track on how many times the command has been launched
                    # also store the full command in case it needs to be restarted later (by the job_monitor() thread)
                    job.utilities[utcmd] = [proc1, 1, utilitycommand]

        return proc

    def wait_graceful(self, args, proc, job):
        """
        (add description)

        :param args:
        :param proc:
        :param job:
        :return:
        """

        log = get_logger(job.jobid)

        breaker = False
        exit_code = None
        iteration = 0L
        while True:
            iteration += 1
            for i in xrange(100):
                if args.graceful_stop.is_set():
                    breaker = True
                    log.debug('breaking -- sending SIGTERM pid=%s' % proc.pid)
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    # proc.terminate()
                    break
                time.sleep(0.1)
            if breaker:
                log.debug('breaking -- sleep 3s before sending SIGKILL pid=%s' % proc.pid)
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
        log = get_logger(str(self.__job.jobid))

        exit_code = 1
        if self.setup_payload(self.__job, self.__out, self.__err):
            log.debug('running payload')
            self.__job.state = 'running'
            send_state(self.__job, self.__args, self.__job.state)
            proc = self.run_payload(self.__job, self.__out, self.__err)
            if proc is not None:
                log.info('will wait for graceful exit')
                exit_code = self.wait_graceful(self.__args, proc, self.__job)
                log.info('finished pid=%s exit_code=%s' % (proc.pid, exit_code))
                self.__job.state = 'finished' if exit_code == 0 else 'failed'

                # write time stamps to pilot timing file
                add_to_pilot_timing(self.__job.jobid, PILOT_POST_PAYLOAD, time.time())

                # stop any running utilities
                if self.__job.utilities != {}:
                    for utcmd in self.__job.utilities.keys():
                        utproc = self.__job.utilities[utcmd][0]
                        if utproc:
                            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
                            user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user],
                                              -1)
                            sig = user.get_utility_command_kill_signal(utcmd)
                            log.info("stopping process \'%s\' with signal %d" % (utcmd, sig))
                            os.killpg(os.getpgid(utproc.pid), sig)

                            user.post_utility_command_action(utcmd, self.__job)

        return exit_code
