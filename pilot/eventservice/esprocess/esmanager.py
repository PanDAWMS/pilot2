#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017

import logging

from pilot.eventservice.esprocess.esprocess import ESProcess
from pilot.eventservice.esprocess.eshook import ESHook

logger = logging.getLogger(__name__)

"""
ES manager to setup and run ESProcess.
"""


class ESManager:
    def __init__(self, hook):
        """
        Initialization: setup ES hooks.

        :param hook: an instance of ESHook.
        """
        logger.info('initializing hooks')
        if not isinstance(hook, ESHook):
            raise Exception("hook(%s) is not instance of %s" % (hook, ESHook))

        self.__hook = hook
        logger.info('initialized hooks')

    def run(self):
        """
        Initialize and run ESProcess.
        """

        logger.debug('gettting payload')
        payload = self.__hook.get_payload()
        logger.debug('got payload: %s' % payload)

        logger.info('init ESProcess')
        process = ESProcess(payload)
        process.set_get_event_ranges_hook(self.__hook.get_event_ranges)
        process.set_handle_out_message_hook(self.__hook.handle_out_message)

        logger.info('ESProcess starts to run')
        process.start()
        process.join()
        logger.info('ESProcess finishes')
