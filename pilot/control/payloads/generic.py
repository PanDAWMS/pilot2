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
# - Wen Guan, wen.guan@cern.ch, 2018

import time
import os

from pilot.control.job import send_state
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


class Executor(object):
    def __init__(self, args, job, out, err):
        self.__args = args
        self.__job = job
        self.__out = out
        self.__err = err

    def setup_payload(self, job, out, err):
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

    def run_payload(self, job, out, err):
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

    def wait_graceful(self, args, proc, job):
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
                send_state(job, args, 'running')
                continue

        return exit_code

    def run(self):
        """
        (add description)

        :return:
        """
        log = logger.getChild(str(self.__job['PandaID']))

        exit_code = 1
        if self.setup_payload(self.__job, self.__out, self.__err):
            log.debug('running payload')
            send_state(self.__job, self.__args, 'running')
            proc = self.run_payload(self.__job, self.__out, self.__err)
            if proc is not None:
                exit_code = self.wait_graceful(self.__args, proc, self.__job)
                log.info('finished pid=%s exit_code=%s' % (proc.pid, exit_code))
        return exit_code
