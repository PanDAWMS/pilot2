# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019


"""
The implmemtation of high-level Info Service module,
which includes a set of low-level information providers to aggregate, prioritize (overwrite),
hide dependency to external storages and expose (queue, site, storage, etc) details
in a unified structured way via provided high-level API

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import inspect

from pilot.common.exception import PilotException, NotDefined, QueuedataFailure

from .configinfo import PilotConfigProvider
from .extinfo import ExtInfoProvider
# from .jobinfo import JobInfoProvider

from .dataloader import merge_dict_data
from .queuedata import QueueData
from .storagedata import StorageData

import logging
logger = logging.getLogger(__name__)


class InfoService(object):
    """
        High-level Information Service
    """

    cache_time = 60  # default cache time in seconds

    def require_init(func):  # noqa
        """
            Method decorator to check if object is initialized
        """
        key = 'pandaqueue'

        def inner(self, *args, **kwargs):
            if getattr(self, key, None) is None:
                raise PilotException("failed to call %s(): InfoService instance is not initialized. Call init() first!" % func.__name__)
            return func(self, *args, **kwargs)

        return inner

    def __init__(self):

        self.pandaqueue = None
        self.queuedata = None   ## cache instance of QueueData for PandaQueue settings

        self.queues_info = {}    ## cache of QueueData objects for PandaQueue settings
        self.storages_info = {}   ## cache of QueueData objects for DDMEndpoint settings
        #self.sites_info = {}     ## cache for Site settings

        self.confinfo = None   ## by default (when non initalized) ignore overwrites/settings from Config
        self.jobinfo = None    ## by default (when non initalized) ignore overwrites/settings from Job
        self.extinfo = ExtInfoProvider(cache_time=self.cache_time)

        self.storage_id2ddmendpoint = {}
        self.ddmendpoint2storage_id = {}

    def init(self, pandaqueue, confinfo=None, extinfo=None, jobinfo=None):

        self.confinfo = confinfo or PilotConfigProvider()
        self.jobinfo = jobinfo  # or JobInfoProvider()
        self.extinfo = extinfo or ExtInfoProvider(cache_time=self.cache_time)

        self.pandaqueue = pandaqueue

        if not self.pandaqueue:
            raise PilotException('Failed to initialize InfoService: panda queue name is not set')

        self.queues_info = {}     ##  reset cache data
        self.storages_info = {}   ##  reset cache data
        #self.sites_info = {}     ##  reset cache data

        self.queuedata = self.resolve_queuedata(self.pandaqueue)

        if not self.queuedata or not self.queuedata.name:
            raise QueuedataFailure("Failed to resolve queuedata for queue=%s, wrong PandaQueue name?" % self.pandaqueue)

        self.resolve_storage_data()  ## prefetch details for all storages

    @classmethod
    def whoami(self):
        """
            :return: Current function name being executed
        """
        return inspect.stack()[1][3]

    @classmethod
    def _resolve_data(self, fname, providers=[], args=[], kwargs={}, merge=False):
        """
            Resolve data by calling function `fname` of passed provider objects.

            Iterate over `providers`, merge data from all providers if merge is True,
            (consider 1st success result from prioritized list if `merge` mode is False)
            and resolve data by execution function `fname` with passed arguments `args` and `kwargs`

            :return: The result of first successfull execution will be returned
        """

        ret = None
        if merge:
            providers = list(providers)
            providers.reverse()
        for provider in providers:
            fcall = getattr(provider, fname, None)
            if callable(fcall):
                try:
                    r = fcall(*(args or []), **(kwargs or {}))
                    if not merge:
                        return r
                    ret = merge_dict_data(ret or {}, r or {})
                except Exception as e:
                    logger.warning("failed to resolve data (%s) from provider=%s .. skipped, error=%s" % (fcall.__name__, provider, e))
                    import traceback
                    logger.warning(traceback.format_exc())

        return ret

    @require_init
    def resolve_queuedata(self, pandaqueue):  ## high level API
        """
            Resolve final full queue data details

            :param pandaqueue: name of PandaQueue
            :return: `QueueData` object or None if not exist
        """

        cache = self.queues_info

        if pandaqueue not in cache:  # not found in cache: do load and initialize data

            # the order of providers makes the priority
            r = self._resolve_data(self.whoami(), providers=(self.confinfo, self.jobinfo, self.extinfo), args=[pandaqueue],
                                   kwargs={'schedconf_priority': self.resolve_schedconf_sources()},
                                   merge=True)
            queuedata = r.get(pandaqueue)
            if queuedata:
                cache[pandaqueue] = QueueData(queuedata)

        return cache.get(pandaqueue)

    #@require_init
    def resolve_storage_data(self, ddmendpoints=[]):  ## high level API
        """
            :return: dict of DDMEndpoint settings by DDMEndpoint name as a key
        """

        try:
            if isinstance(ddmendpoints, basestring):  # Python 2
                ddmendpoints = [ddmendpoints]
        except Exception:
            if isinstance(ddmendpoints, str):  # Python 3
                ddmendpoints = [ddmendpoints]

        cache = self.storages_info

        miss_objs = set(ddmendpoints) - set(cache)
        if not ddmendpoints or miss_objs:  # not found in cache: do load and initialize data
            # the order of providers makes the priority
            r = self._resolve_data(self.whoami(), providers=(self.confinfo, self.jobinfo, self.extinfo),
                                   args=[miss_objs], merge=True)
            if ddmendpoints:
                not_resolved = set(ddmendpoints) - set(r)
                if not_resolved:
                    raise PilotException("internal error: Failed to load storage details for ddms=%s" % sorted(not_resolved))
            for ddm in r:
                cache[ddm] = StorageData(r[ddm])

        return cache

    @require_init
    def resolve_schedconf_sources(self):  ## high level API
        """
            Resolve prioritized list of source names for Schedconfig data load
            Consider first the config settings of pilot instance (via `confinfo`)
            and then Job specific settings (via `jobinfo` instance),
            and failover to default value (LOCAL, CVMFS, AGIS, PANDA)
        """

        defval = ['LOCAL', 'CVMFS', 'CRIC', 'PANDA']

        # look up priority order: either from job, local config or hardcoded in the logic
        return self._resolve_data(self.whoami(), providers=(self.confinfo, self.jobinfo)) or defval

    #@require_init
    #def resolve_field_value(self, name): ## high level API
    #
    #    """
    #        Return the value from the given schedconfig field
    #
    #        :param field: schedconfig field (string, e.g. catchall)
    #        :return: schedconfig field value (string)
    #    """
    #
    #    # look up priority order: either from job, local config, extinfo provider
    #    return self._resolve_data(self.whoami(), providers=(self.confinfo, self.jobinfo, self.extinfo), args=[name])

    def resolve_ddmendpoint_storageid(self, ddmendpoint=[]):
        """
        Resolve the map between ddmendpoint and storage_id
        """
        if not ddmendpoint or ddmendpoint not in self.ddmendpoint2storage_id:
            storages = self.resolve_storage_data(ddmendpoint)
            for storage_name in storages:
                storage = storages[storage_name]
                storage_id = storage.pk
                self.ddmendpoint2storage_id[storage_name] = storage_id
                self.storage_id2ddmendpoint[storage_id] = storage_name
                if storage.resource:
                    bucket_id = storage.resource.get('bucket_id', None)
                    if bucket_id:
                        self.storage_id2ddmendpoint[bucket_id] = storage_name

    def get_storage_id(self, ddmendpoint):
        """
        Return the storage_id of a ddmendpoint.

        :param ddmendpoint: ddmendpoint name.
        :returns storage_id: storage_id of the ddmendpoint.
        :raises NotDefined:
        """
        if ddmendpoint not in self.ddmendpoint2storage_id:
            self.resolve_ddmendpoint_storageid(ddmendpoint)

        if ddmendpoint in self.ddmendpoint2storage_id:
            storage_id = self.ddmendpoint2storage_id[ddmendpoint]
            logger.info("Found storage id for ddmendpoint(%s): %s" % (ddmendpoint, storage_id))
            return storage_id
        else:
            raise NotDefined("Cannot find the storage id for ddmendpoint: %s" % ddmendpoint)

    def get_ddmendpoint(self, storage_id):
        """
        Return the ddmendpoint name from a storage id.

        :param storage_id: storage_id as an int.
        :returns ddmendpoint: ddmendpoint name.
        :raises NotDefined:
        """
        storage_id = int(storage_id)
        if storage_id not in self.storage_id2ddmendpoint:
            self.resolve_ddmendpoint_storageid()

        if storage_id in self.storage_id2ddmendpoint:
            ddmendpoint = self.storage_id2ddmendpoint[storage_id]
            logger.info("Found ddmendpoint for storage id(%s): %s" % (storage_id, ddmendpoint))
            return ddmendpoint
        else:
            self.resolve_storage_data()
            raise NotDefined("Cannot find ddmendpoint for storage id: %s" % storage_id)
