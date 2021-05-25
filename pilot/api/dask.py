#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

#from pilot.common.exception import NotDefined, NotSameLength, UnknownException
#from pilot.util.filehandling import get_table_from_file
#from pilot.util.math import mean, sum_square_dev, sum_dev, chi2, float_to_rounded_string
from pilot.util.container import execute
from pilot.util.filehandling import establish_logging, write_file
from pilot.util.parameters import convert_to_int

import os
import re

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
                logger.info('service %s is running' % name)
            else:
                logger.info('service %s is not yet running' % name)

    def is_running(self, name='single-dask-scheduler'):
        """

        """

        status = False
        dictionary = self._get_dictionary(cmd='kubectl get services')
        logger.debug('d=%s' % str(dictionary))
        for key in dictionary:
            if key == name:
                logger.debug('ip:%s' % dictionary[key]['EXTERNAL-IP'])
                status = True if self._is_valid_ip(dictionary[key]['EXTERNAL-IP']) else False
                logger.debug('status=%s' % str(status))
                break

        return status

    def _is_valid_ip(self, ip):
        """

        """

        regex = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"
        return True if re.search(regex, ip) else False

    def _get_dictionary(self, cmd=None):
        """

        """

        dictionary = {}
        if not cmd:
            return dictionary

        exit_code, stdout, stderr = execute(cmd, mute=True)
        if exit_code:
            logger.warning('failed to execute \'%s\': %s' % (cmd, stdout))
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

        # import relevant modules
        try:
            import dask
            logger.debug('dask imported')
            import dask_kubernetes
            logger.debug('dask_kubernetes imported')
        except Exception as error:
            logger.warning('module not available: %s' % error)
            return False

        # verify relevant commands
        commands = ['helm', 'kubectl']
        found = False
        for cmd in commands:
            exit_code, stdout, stderr = execute('which %s' % cmd, mute=True)
            found = True if not 'not found' in stdout else False
            if not found:
                logger.warning(stdout)
                break
            else:
                logger.debug('%s verified' % cmd)
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
            logger.info('file \'%s\' already exists - will not override' % filename)
            return

        script = ""
        if not jupyter:
            script += 'jupyter:\n    enabled: false\n\n'
        if servicetype:
            script += 'scheduler:\n    serviceType: \"%s\"\n' % servicetype

        status = write_file(filename, script)
        if status:
            logger.debug('generated script: %s' % filename)

    def _convert_to_dict(self, output):
        """

        """

        dictionary = {}
        first_line = []
        for line in output.split('\n'):
            logger.debug('line=%s' % line)
            try:
                # Remove empty entries from list (caused by multiple \t)
                _l = line
                _l = [_f for _f in _l.split('\t') if _f]
                logger.debug('_l=%s' % _l)
                if first_line == []:  # "NAME TYPE CLUSTER-IP EXTERNAL-IP PORT(S) AGE
                    first_line = _l[1:]
                    logger.debug('first line=%s' % first_line)
                else:
                    dictionary[_l[0]] = {}
                    for i in range(len(_l[1:])):
                        dictionary[_l[0]][first_line[i]] = _l[1:][i]

            except Exception:
                logger.warning("unexpected format of utility output: %s" % line)

        logger.debug('dictionary=%s' % str(dictionary))
        return dictionary
