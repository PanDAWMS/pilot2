#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018

import os
import logging

from pilot.info import infosys
from pilot.common.exception import PilotException


class StagingClient(object):
    def __init__(self, site=None, ddmendpoint=None, copytool_names=None, fallback_copytool='rucio', infosys_instance=None, logger=None):
        """
        StagingClient constructor needs either copytool_names or ddmendpoint specified

        :param site: vo site
        :param ddmendpoint: ddmendpoint where the copytool settings are stored
        :param copytool_names: name of copytool or list of copytools to use (if this is, given ddmendpoint will be ignored)
        :param fallback_copytool: name or list of copytools to use if storage settings cannot be retrieved
        :param logger: logging.Logger object to use for loggin (None means no logging)
        :return:
        """
        super(StagingClient, self).__init__()

        self.copytool_modules = {}
        self._fill_copytool_modules()

        if not logger:
            logger = logging.getLogger('%s.%s' % (__name__, 'null'))
            logger.disabled = True
        self.logger = logger

        if not copytool_names and not ddmendpoint:
            raise PilotException('Invalid arguments passed to StagingClient.__init__')

        self.infosys = infosys_instance if infosys_instance else infosys

        self.copytool_names = []
        if not copytool_names:
            # try to get the copytools from the storage endpoint config
            try:
                storage_data = self.infosys.resolve_storage_data(ddmendpoint).get(ddmendpoint)
                acopytools = storage_data.acopytools.get('read_lan')
                if acopytools and len(acopytools):
                    self.copytool_names = acopytools
            except Exception:
                logger.warning('Failed to get copytool from storage endpoint configuration. Using fallback.')
        else:
            # given copytools are used instead of storage endpoint configured copytools
            if isinstance(copytool_names, basestring):
                copytool_names = [copytool_names]
            self.copytool_names = copytool_names

        # if we failed getting the copytools, use rucio as default
        if not len(self.copytool_names):
            if isinstance(fallback_copytool, basestring):
                fallback_copytool = [fallback_copytool]
            self.copytool_names = fallback_copytool

        logger.debug('Copytool options: %s' % self.copytool_names)

        # Check validity of specified site - should be refactored into VO-agnostic setup
        self.site = os.environ.get('VO_ATLAS_AGIS_SITE', site)
        if self.site is None and self.copytool_names == ['rucio']:
            raise PilotException('VO_ATLAS_AGIS_SITE not available, must set StageInClient(site=...) parameter')

    def _fill_copytool_modules(self):
        self.copytool_modules = {'rucio': {'module_name': 'rucio'},
                                 'gfal': {'module_name': 'gfal'},
                                 'gfalcopy': {'module_name': 'gfal'},
                                 'lcgcp': {'module_name': 'lcgcp'},
                                 'dccp': {'module_name': 'dccp'},
                                 'xrdcp': {'module_name': 'xrdcp'},
                                 'mv': {'module_name': 'mv'}
                                }

    def _try_copytool_for_transfer(self, copytool, files):
        """
        Try to transfer files with given copytool
        Needs to be implemented by subclasses

        :param copytool: copytool to try
        :param files: files to transfer
        :return: output of the given copytool or None on error
        """
        raise NotImplementedError

    def transfer(self, files):
        logger = self.logger
        copytool_names = self.copytool_names[::-1]
        output = None
        while len(copytool_names) and output is None:
            copytool = None
            copytool_name = copytool_names.pop()
            module_name = self.copytool_modules[copytool_name]['module_name']
            logger.info('Trying to use copytool %s' % copytool_name)
            try:
                copytool = __import__('pilot.copytool.%s' % module_name,
                                      globals(), locals(),
                                      [module_name], -1)
            except Exception as error:
                logger.warning('Failed to import copytool %s' % module_name)
                logger.debug('Error: %s' % error)
                continue

            output = self._try_copytool_for_transfer(copytool, files)

        if output is None:
            raise PilotException('transfer failed')
        return output


class StageInClient(StagingClient):
    def __init__(self, site=None, ddmendpoint=None, copytool_names=None, fallback_copytool='rucio', infosys_instance=None, logger=None):
        """
        StageInClient constructor needs either copytool_names or ddmendpoint specified

        :param site: vo site
        :param ddmendpoint: ddmendpoint where the copytool settings are stored
        :param copytool_names: name of copytool or list of copytools to use (if this is, given ddmendpoint will be ignored)
        :param fallback_copytool: name or list of copytools to use if storage settings cannot be retrieved
        :param logger: logging.Logger object to use for loggin (None means no logging)
        :return:
        """
        super(StageInClient, self).__init__(site, ddmendpoint, copytool_names, fallback_copytool, infosys_instance, logger)

    def _try_copytool_for_transfer(self, copytool, files):
        """
        Automatically stage in files using the selected copy tool.

        :param copytool: copytool to try
        :param files: List of dictionaries containing the file information

        :return: the output of the copytool or None on error
        """
        logger = self.logger
        try:
            if not copytool.is_valid_for_copy_in(files):
                logger.warning('Input is not valid for this copytool')
                logger.debug('Input: %s' % files)
                return None
            return copytool.copy_in(files)
        except Exception as error:
            logger.warning('Failed transferring files with this copytool')
            logger.debug('Error: %s' % error)
        return None


class StageOutClient(StagingClient):
    def __init__(self, site=None, ddmendpoint=None, copytool_names=None, fallback_copytool='rucio', infosys_instance=None, logger=None):
        """
        StageOutClient constructor needs either copytool_names or ddmendpoint specified

        :param site: vo site
        :param ddmendpoint: ddmendpoint where the copytool settings are stored
        :param copytool_names: name of copytool or list of copytools to use (if this is, given ddmendpoint will be ignored)
        :param fallback_copytool: name or list of copytools to use if storage settings cannot be retrieved
        :param logger: logging.Logger object to use for loggin (None means no logging)
        """
        super(StageOutClient, self).__init__(site, ddmendpoint, copytool_names, fallback_copytool, infosys_instance, logger)

    def _try_copytool_for_transfer(self, copytool, files):
        """
        Automatically stage out files using rucio.

        :param copytool: copytool to try for the transfer
        :param files: List of dictionaries containing the target scope, the path to the file, and destination RSE

        :return: the output of the copytool or None on error
        """
        logger = self.logger
        try:
            if not copytool.is_valid_for_copy_out(files):
                logger.warning('Input is not valid for this copytool')
                logger.debug('Input: %s' % files)
                return None
            return copytool.copy_out(files)
        except Exception as error:
            logger.warning('Failed transferring files with this copytool')
            logger.debug('Error: %s' % error)
        return None


class StageInClientAsync(object):
    def __init__(self, site):
        raise NotImplementedError

    def queue(self, files):
        raise NotImplementedError

    def is_transferring(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def finish(self):
        raise NotImplementedError

    def status(self):
        raise NotImplementedError


class StageOutClientAsync(object):
    def __init__(self, site):
        raise NotImplementedError

    def queue(self, files):
        raise NotImplementedError

    def is_transferring(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def finish(self):
        raise NotImplementedError

    def status(self):
        raise NotImplementedError
