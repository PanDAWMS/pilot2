#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern,ch, 2018

import traceback
import logging

from pilot.info.storagemaps import storage_maps
from pilot.common.exception import PilotException, ErrorCodes
from pilot.api.data import StagingClient, StageInClient, StageOutClient

logger = logging.getLogger(__name__)


class StagingESClient(StagingClient):
    """
        Base ES Staging Client
    """

    def __init__(self, infosys_instance=None, acopytools=None, logger=logger, default_copytools='rucio', default_activity='default'):
        """
            If `acopytools` is not specified then it will be automatically resolved via infosys. In this case `infosys` requires initialization.
            :param acopytools: dict of copytool names per activity to be used for transfers. Accepts also list of names or string value without activity passed.
            :param logger: logging.Logger object to use for logging (None means no logging)
            :param default_copytools: copytool name(s) to be used in case of unknown activity passed. Accepts either list of names or single string value.
            "param default_activity: default activity name
        """

        super(StagingESClient, self).__init__(infosys_instance=infosys_instance, acopytools=acopytools, logger=None, default_copytools=default_copytools)

        self.copytool_modules['objectstore'] = {'module_name': 'objectstore'}

        if 'es_events' not in self.acopytools:
            self.acopytools['es_events'] = ['objectstore']
        if 'es_events_read' not in self.acopytools:
            self.acopytools['es_events_read'] = ['objectstore']

        self.astorages = {}
        if self.infosys and self.infosys.queuedata and self.infosys.queuedata.astorages:
            self.astorages = (self.infosys.queuedata.astorages or {}).copy()

        # es_events_read should be able to read from all storages
        if 'es_events_read' not in self.astorages:
            self.astorages['es_events_read'] = []

        logger.info('Configured astorages per activity: astorages=%s' % self.astorages)

    def transfer(self, files, activity=['pw'], **kwargs):  # noqa: C901
        """
            Automatically stage passed files using copy tools related to given `activity`
            :param files: list of `FileSpec` objects
            :param activity: list of activity names used to determine appropriate copytool (prioritized list)
            :param kwargs: extra kwargs to be passed to copytool transfer handler
            :raise: PilotException in case of controlled error
            :return: output of copytool trasfers (to be clarified)
        """

        if isinstance(activity, basestring):
            activity = [activity]

        result, errors = None, []
        avail_activity = False
        for act in activity:
            copytools = self.acopytools.get(act)
            storages = self.astorages.get(act)
            if not copytools:
                logger.warn("No available copytools for activity %s" % act)
                continue
            if act in ['pw', 'pls', 'es_events', 'es_failover'] and not storages:
                # for write activity, if corresponding storages are not defined, should use different activity
                logger.warn("Failed to find corresponding astorages for writing activity(%s), will try next activity" % act)
                continue

            storage = storages[0] if storages else None
            avail_activity = True
            for name in copytools:
                try:
                    if name not in self.copytool_modules:
                        raise PilotException('passed unknown copytool with name=%s .. skipped' % name)
                    module = self.copytool_modules[name]['module_name']
                    logger.info('Trying to use copytool=%s for activity=%s' % (name, act))
                    copytool = __import__('pilot.copytool.%s' % module, globals(), locals(), [module], -1)
                except PilotException as e:
                    errors.append(e)
                    logger.debug('Error: %s' % e)
                    continue
                except Exception as e:
                    logger.warning('Failed to import copytool module=%s, error=%s' % (module, e))
                    logger.debug('Error: %s' % e)
                    continue

                try:
                    result = self.transfer_files(copytool, files, act, storage, **kwargs)
                except PilotException, e:
                    errors.append(e)
                    logger.debug('Error: %s' % e)
                except Exception as e:
                    logger.warning('Failed to transfer files using copytool=%s .. skipped; error=%s' % (copytool, e))
                    logger.error(traceback.format_exc())
                    errors.append(e)

                if errors and isinstance(errors[-1], PilotException) and errors[-1].get_error_code() == ErrorCodes.MISSINGOUTPUTFILE:
                    raise errors[-1]

                if result:
                    break
                else:
                    logger.warn("Failed to transfer files using activity(%s) copytool(%s) with error=%s" % (act, name, errors))
            if result:
                break
            else:
                logger.warn("Failed to transfer files using activity(%s) with copytools(%s)" % (act, copytools))

        if not avail_activity:
            raise PilotException('Not available activity with both acopytools and astorages defined')
        if not result:
            raise PilotException('Failed to transfer files with activities %s' % (activity))

        return result


class StageInESClient(StagingESClient, StageInClient):

    def get_storage_id_and_path_convention(self, storage_token):
        """
        Parse storage_token to get storage_id and path_convention.

        :param storage_token: string,Expected format is '<normal storage token as string>', '<storage_id as int>', <storage_id as int/path_convention as int>
        :returns: storage_id, path_convention
        """

        storage_id = None
        path_convention = None
        try:
            if storage_token:
                if storage_token.count('/') == 1:
                    storage_id, path_convention = storage_token.split('/')
                    storage_id = int(storage_id)
                    path_convention = int(path_convention)
                elif storage_token.isdigit():
                    storage_id = int(storage_token)
        except Exception as ex:
            logger.warning("Failed to parse storage_token(%s): %s, %s" % (storage_token, ex, traceback.format_exc()))
        return storage_id, path_convention

    def process_storage_id(self, files):
        """
        If storage_id is specified, replace ddmendpoint by parsing storage_id
        """
        for fspec in files:
            if fspec.storage_token:
                storage_id, path_convention = self.get_storage_id_and_path_convention(fspec.storage_token)
                if path_convention and path_convention == 1000:
                    fspec.scope = 'transient'
                if storage_id:
                    fspec.ddmendpoint = storage_maps.get_ddmendpoint(storage_id)
                logger.info("Processed file with storage id: %s" % fspec)

    def transfer_files(self, copytool, files, activity, ddmendpoint, **kwargs):
        """
            Automatically stage in files using the selected copy tool module.

            :param copytool: copytool module
            :param files: list of `FileSpec` objects
            :param activity: activity name.
            :param ddmendpoint: ddmendpoint name.
            :param kwargs: extra kwargs to be passed to copytool transfer handler

            :return: the output of the copytool transfer operation
            :raise: PilotException in case of controlled error
        """
        self.process_storage_id(files)
        return super(StageInESClient, self).transfer_files(copytool, files, activity=activity, ddmendpoint=ddmendpoint, **kwargs)


class StageOutESClient(StagingESClient, StageOutClient):

    def transfer_files(self, copytool, files, activity, ddmendpoint, **kwargs):
        """
            Automatically stage out files using the selected copy tool module.

            :param copytool: copytool module
            :param files: list of `FileSpec` objects
            :param activity: activity name.
            :param ddmendpoint: ddmendpoint name.
            :param kwargs: extra kwargs to be passed to copytool transfer handler

            :return: the output of the copytool transfer operation
            :raise: PilotException in case of controlled error
        """
        logger.info("To transfer files with activity: %s" % (activity))
        for fspec in files:
            if not fspec.ddmendpoint or fspec.ddmendpoint != ddmendpoint:
                logger.info("Based on activity %s, changing ddmendpoint from '%s' for file(%s) to '%s'" % (activity, fspec.ddmendpoint, fspec.lfn, ddmendpoint))
                fspec.ddmendpoint = ddmendpoint

        return super(StageOutESClient, self).transfer_files(copytool, files, activity, **kwargs)
