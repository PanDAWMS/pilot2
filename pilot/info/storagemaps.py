# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018


import logging

from pilot.common import exception
from pilot.info import infosys

logger = logging.getLogger(__name__)


class StorageMaps(object):
    """
    Singleton class to manage the map between storage_id and ddmendpoint
    """

    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance

    def __init__(self, ddmendpoints):
        """
        Init the map.

        :param ddmendpoints: dictionary of storage data
        """
        self.storage_id2ddmendpoint = {}
        self.ddmendpoint2storage_id = {}
        for ddmendpoint in ddmendpoints:
            storage_id = ddmendpoints[ddmendpoint].pk
            self.ddmendpoint2storage_id[ddmendpoint] = storage_id
            self.storage_id2ddmendpoint[storage_id] = ddmendpoint

    def get_storage_id(self, ddmendpoint):
        """
        Return the storage_id of a ddmendpoint.

        :param ddmendpoint: ddmendpoint name.
        :returns storage_id: storage_id of the ddmendpoint.
        :raises NotDefined:
        """
        if ddmendpoint in self.ddmendpoint2storage_id:
            storage_id = self.ddmendpoint2storage_id[ddmendpoint]
            logger.info("Found storage id for ddmendpoint(%s): %s" % (ddmendpoint, storage_id))
            return storage_id
        else:
            raise exception.NotDefined("Cannot find the storage id for ddmendpoint: %s" % ddmendpoint)

    def get_ddmendpoint(self, storage_id):
        """
        Return the ddmendpoint name from a storage id.

        :param storage_id: storage_id as an int.
        :returns ddmendpoint: ddmendpoint name.
        :raises NotDefined:
        """
        storage_id = int(storage_id)
        if storage_id in self.storage_id2ddmendpoint:
            ddmendpoint = self.storage_id2ddmendpoint[storage_id]
            logger.info("Found ddmendpoint for storage id(%s): %s" % (storage_id, ddmendpoint))
            return ddmendpoint
        else:
            raise exception.NotDefined("Cannot find ddmendpoint for storage id: %s" % storage_id)


storage_maps = StorageMaps(infosys.resolve_storage_data())
