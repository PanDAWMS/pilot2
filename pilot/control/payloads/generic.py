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

import subprocess
import time

from pilot.control.job import send_state

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

        :param job:
        :param out:
        :param err:
        :return:
        """
        log = logger.getChild(str(job['PandaID']))

        # get the payload command from the user specific code
        # cmd = get_payload_command(job, queuedata)
        athena_version = job['homepackage'].split('/')[1]
        asetup = 'source $ATLAS_LOCAL_ROOT_BASE/user/atlasLocalSetup.sh --quiet; '\
                 'source $AtlasSetup/scripts/asetup.sh %s,here; ' % athena_version
        cmd = job['transformation'] + ' ' + job['jobPars']

        log.debug('executable=%s' % asetup + cmd)

        try:
            proc = subprocess.Popen(asetup + cmd,
                                    bufsize=-1,
                                    stdout=out,
                                    stderr=err,
                                    cwd=job['working_dir'],
                                    shell=True)
        except Exception as e:
            log.error('could not execute: %s' % str(e))
            return None

        log.info('started -- pid=%s executable=%s' % (proc.pid, asetup + cmd))

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
                exit_code = self.wait_graceful(self.__args, self.__proc, self.__job)
                log.info('finished pid=%s exit_code=%s' % (proc.pid, exit_code))
        return exit_code
