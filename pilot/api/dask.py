#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

try:
    # import dask
    import dask_kubernetes
#except ModuleNotFoundError:  # Python 3
except Exception:
    pass

#from pilot.common.exception import NotDefined, NotSameLength, UnknownException
from pilot.util.container import execute
from pilot.util.filehandling import establish_logging, write_file

import os
import re
from time import sleep

import logging
logger = logging.getLogger(__name__)


class Dask(object):
    """
    Dask interface class.
    """

    servicename = 'single-dask'
    status = None
    loadbalancerip = None
    servicetype = "LoadBalancer"
    jupyter = False
    overrides = "override_values.yaml"
    _workdir = os.getcwd()
    cluster = None

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        """

        _servicename = kwargs.get('servicename', None)
        if _servicename:
            self.servicename = _servicename
        _servicetype = kwargs.get('servicetype', None)
        if _servicetype:
            self.servicetype = _servicetype
        _jupyter = kwargs.get('jupyter', None)
        if _jupyter:
            self.jupyter = _jupyter
        _overrides = kwargs.get('overrides', None)
        if _overrides:
            self.overrides = _overrides

    def uninstall(self, block=True):
        """

        """

        logger.info('uninstalling service %s', self.servicename)
        if block:
            logger.warning('blocking mode not yet implemented')

        cmd = 'helm uninstall %s' % self.servicename
        exit_code, stdout, stderr = execute(cmd, mute=True)
        if not exit_code:
            self.status = 'uninstalled'
            logger.info('uninstall of service %s has been requested', self.servicename)

    def install(self, block=True):
        """

        """

        # can dask be installed?
        if not self._validate():
            logger.warning('validation failed')
            self.status = 'failed'
        else:
            logger.debug('dask has been validated')
            self.status = 'validated'

            # is the single-dask cluster already running?
            name = '%s-scheduler' % self.servicename
            if self.is_running(name=name):
                logger.info('service %s is already running - nothing to install', name)
            else:
                logger.info('service %s is not yet running - proceed with installation', name)

                # perform helm updates before actual instqllation
                cmd = ''
                #
                override_option = "-f %s" % self.overrides if self.overrides else ""
                cmd = 'helm install %s %s dask/dask' % (override_option, self.servicename)
                exit_code, stdout, stderr = execute(cmd, mute=True)
                if not exit_code:
                    logger.info('installation of service %s is in progress', self.servicename)

                    if block:
                        while True:
                            name = '%s-scheduler' % self.servicename
                            if self.is_running(name=name):
                                logger.info('service %s is running', name)
                                self.status = 'running'
                                break
                            else:
                                self.status = 'pending'
                                sleep(2)
                    # note: in non-blocking mode, status is not getting updated

    def is_running(self, name='single-dask-scheduler'):
        """

        """

        status = False
        dictionary = self._get_dictionary(cmd='kubectl get services')
        for key in dictionary:
            if key == name:
                status = True if self._is_valid_ip(dictionary[key]['EXTERNAL-IP']) else False
                break

        return status

    def _is_valid_ip(self, ip):
        """
        Verify that the given IP number is valid.

        :param ip: IP number (string).
        :return: Boolean.
        """

        regex = r"^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"
        return True if re.search(regex, ip) else False

    def _get_dictionary(self, cmd=None):
        """

        """

        dictionary = {}
        if not cmd:
            return dictionary

        exit_code, stdout, stderr = execute(cmd, mute=True)
        if exit_code:
            logger.warning('failed to execute \'%s\': %s', cmd, stdout)
            self.status = 'failed'
        else:
            # parse output
            dictionary = self._convert_to_dict(stdout)

        return dictionary

    def _validate(self):
        """
        Make sure that pre-conditions are met before any installation can be attempted.

        Pre-conditions: required libraries and commands
        1. library: dask
        2. library: dask_kubernetes
        3. command: helm
        4. command: kubectl
        5. copy relevant yaml file(s)
        """

        establish_logging(debug=True)

        # check imported modules
        # dask
        # dask_kubernetes

        # verify relevant commands
        commands = ['helm', 'kubectl']
        found = False
        for cmd in commands:
            exit_code, stdout, stderr = execute('which %s' % cmd, mute=True)
            found = True if 'not found' not in stdout else False
            if not found:
                logger.warning(stdout)
                break
            else:
                logger.debug('%s verified', cmd)
        if not found:
            return False

        # create yaml file(s)
        self._generate_override_script()

        return True

    def _generate_override_script(self, jupyter=False, servicetype='LoadBalancer'):
        """
        Generate a values yaml script, unless it already exists.

        :param jupyter: False if jupyter notebook server should be disabled (Boolean).
        :param servicetype: name of service type (string).
        :return:
        """

        filename = os.path.join(self._workdir, self.overrides)
        if os.path.exists(filename):
            logger.info('file \'%s\' already exists - will not override', filename)
            return

        script = ""
        if not jupyter:
            script += 'jupyter:\n    enabled: false\n\n'
        if servicetype:
            script += 'scheduler:\n    serviceType: \"%s\"\n' % servicetype

        if script:
            status = write_file(filename, script)
            if status:
                logger.debug('generated script: %s', filename)
        else:
            self.overrides = None

    def _convert_to_dict(self, output):
        """

        """

        dictionary = {}
        first_line = []
        for line in output.split('\n'):
            try:
                # Remove empty entries from list (caused by multiple \t)
                _l = re.sub(' +', ' ', line)
                _l = [_f for _f in _l.split(' ') if _f]
                if first_line == []:  # "NAME TYPE CLUSTER-IP EXTERNAL-IP PORT(S) AGE
                    first_line = _l[1:]
                else:
                    dictionary[_l[0]] = {}
                    for i in range(len(_l[1:])):
                        dictionary[_l[0]][first_line[i]] = _l[1:][i]

            except Exception:
                logger.warning("unexpected format of utility output: %s", line)

        return dictionary

    def connect_cluster(self, release_name=None, manager=dask_kubernetes.HelmCluster):
        """

        """

        if not release_name:
            release_name = self.servicename
        self.cluster = manager(release_name=release_name)
        logger.info('connected to %s', manager.__name__)

    def scale(self, number):
        """

        """

        if number > 2:
            logger.warning('too large scale: %d (please use <= 2 for now)', number)
            return
        if not self.cluster:
            self.connect_cluster()
        if not self.cluster:
            logger.warning('cluster not connected - cannot proceed')
            self.status = 'failed'
            return

        logger.info('setting scale to: %d', number)
        self.cluster.scale(number)

    def shutdown(self):
        """
        Shutdown logging.

        """

        logging.handlers = []
        logging.shutdown()
