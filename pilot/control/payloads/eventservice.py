#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018


from pilot.control.payloads import generic
from pilot.eventservice.eshook import ESHook
from pilot.eventservice.esprocess import ESProcess

import logging
logger = logging.getLogger(__name__)


class Executor(generic.Executor, ESHook):
    def __init__(self, args, job, out, err):
        super(Executor, self).__init__(args, job, out, err)

    def get_event_ranges(self, num_ranges=1):
        """
        Get event ranges hook function for tests.

        :returns: dict of event ranges.
                  None if no available events.
        """
        ret = []
        # for _ in range(num_ranges):
        #     if len(self.__event_ranges) > 0:
        #         event_range = self.__event_ranges.pop(0)
        #         ret.append(event_range)
        #         self.__injected_event_ranges.append(event_range)
        return ret

    def handle_out_message(self, message):
        """
        Handle ES output or error messages hook function for tests.

        :param message: a dict of parsed message.
                        For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                        Fro 'failed' event ranges, it's {'id': <id>, 'status': 'failed', 'message': <full message>}.
        """

        # print(message)
        pass

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
            payload = {'executable': asetup + cmd, 'workdir': job['working_dir'], 'output_file': out, 'error_file': err}
            proc = ESProcess(payload)
            proc.set_get_event_ranges_hook(self.get_event_ranges)
            proc.set_handle_out_message_hook(self.handle_out_message)

        except Exception as e:
            log.error('could not execute: %s' % str(e))
            return None

        log.info('started -- pid=%s executable=%s' % (proc.pid, asetup + cmd))

        return proc
