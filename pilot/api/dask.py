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

import logging
logger = logging.getLogger(__name__)


class Dask(object):
    """
    Dask interface class.
    """

    status = None
    loadbalancerip = None

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        """

        pass

    def install(self, block=True):
        """

        """

        # can dask be installed?
        if not self._validate():
            logger.warning('validation failed')
            self.status = 'failed'
        else:
            logger.info('dask has been validated')
            self.status = 'validated'

    def _validate(self):
        """
        Make sure that pre-conditions are met before any installation can be attempted.

        Pre-conditions: required libraries and commands
        1. library: dask
        2. library: dask_kubernetes
        3. command: helm
        4. command: kubectl
        """

        try:
            import dask
            import dask_kubernetes
        except Exception as error:
            logger.warning('module not available: %s' % error)
            return False

        commands = ['helm', 'kubectl']
        found = False
        for cmd in commands:
            exit_code, stdout, stderr = execute('which %s' % cmd, mute=True)
            found = True if not 'not found' in stdout else False
            if not found in stdout:
                logger.warning(stdout)
                break
        if not found:
            return False

        return True
