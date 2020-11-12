#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019


import time

from pilot.common import exception
from pilot.common.pluginfactory import PluginFactory

import logging
logger = logging.getLogger(__name__)

"""
Main class to manage the event service work.
"""


class WorkExecutor(PluginFactory):

    def __init__(self, args=None):
        super(WorkExecutor, self).__init__()
        self.payload = None
        self.plugin = None
        self.is_retrieve_payload = False
        self.args = args
        self.pid = None

    def get_pid(self):
        return self.plugin.get_pid() if self.plugin else None

    def set_payload(self, payload):
        self.payload = payload

    def set_retrieve_paylaod(self):
        self.is_retrieve_payload = True

    def get_payload(self):
        return self.payload

    def get_plugin_confs(self):
        plugin_confs = {}
        if self.args and 'executor_type' in list(self.args.keys()):  # Python 2/3
            if self.args['executor_type'] == 'hpo':
                plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.hpoexecutor.HPOExecutor'}
            elif self.args['executor_type'] == 'raythena':
                plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.raythenaexecutor.RaythenaExecutor'}
            elif self.args['executor_type'] == 'generic':
                plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.genericexecutor.GenericExecutor'}
            elif self.args['executor_type'] == 'base':
                plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.baseexecutor.BaseExecutor'}
            elif self.args['executor_type'] == 'nl':  # network-less
                plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.nlexecutor.NLExecutor'}
            elif self.args['executor_type'] == 'boinc':
                plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.boincexecutor.BOINCExecutor'}
            elif self.args['executor_type'] == 'hammercloud':  # hammercloud test: refine normal simul to ES
                plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.hammercloudexecutor.HammerCloudExecutor'}
            elif self.args['executor_type'] == 'mpi':  # network-less
                plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.mpiexecutor.MPIExecutor'}
        else:
            plugin_confs = {'class': 'pilot.eventservice.workexecutor.plugins.genericexecutor.GenericExecutor'}

        plugin_confs['args'] = self.args
        return plugin_confs

    def start(self):
        plugin_confs = self.get_plugin_confs()
        logger.info("Plugin confs: %s" % plugin_confs)
        self.plugin = self.get_plugin(plugin_confs)
        logger.info("WorkExecutor started with plugin: %s" % self.plugin)
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")

        if self.is_retrieve_payload:
            self.payload = self.plugin.set_retrieve_payload()
        else:
            if not self.get_payload():
                raise exception.SetupFailure("Payload is not assigned.")
            else:
                self.plugin.set_payload(self.get_payload())

        logger.info("Starting plugin: %s" % self.plugin)
        self.plugin.start()
        logger.info("Waiting for payload to start")
        while self.plugin.is_alive():
            if self.plugin.is_payload_started():
                logger.info("Payload started with pid: %s" % self.get_pid())
                break
            time.sleep(1)

    def stop(self):
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")
        return self.plugin.stop()

    def is_alive(self):
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")
        return self.plugin.is_alive()

    def get_exit_code(self):
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")
        return self.plugin.get_exit_code()

    def get_event_ranges(self):
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")
        return self.plugin.get_event_ranges()

    def update_events(self, messages):
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")
        return self.plugin.update_events(messages)
