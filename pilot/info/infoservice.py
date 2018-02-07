"""
The implmemtation of high-level Info Service module,
which includes a set of low-level information providers to aggregate, prioritize (overwrite),
hide dependency to external storages and expose (queue, site, storage, etc) details
in a unified structured way via provided high-level API

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import os
import json
import time
import random
import inspect

from .configinfo import PilotConfigProvider
from .extinfo import ExtInfoProvider
from .jobinfo import JobInfoProvider

from .dataloader import DataLoader, merge_dict_data
from .queuedata import QueueData

import logging
logger = logging.getLogger(__name__)


class InfoService(object):
    """
        High-level Information Service
    """

    cache_time = 60 # default cache time in seconds

    def __init__(self, pandaqueue, confinfo=None, extinfo=None, jobinfo=None):

        self.pandaqueue = pandaqueue

        self.confinfo = confinfo or PilotConfigProvider()
        self.extinfo = extinfo or ExtInfoProvider(cache_time=self.cache_time)
        self.jobinfo = jobinfo # or JobInfoProvider()

        self.queuedata = None   ## cache instance of QueueData for PandaQueue settings
        self.queuesconf = {}    ## cache for PandaQueue settings
        self.storageconf = {}   ## cache for DDMConf settings


    def init(self):

        self.queuedata = self.resolve_queuedata(self.pandaqueue)


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
        if merge: ##
            providers = list(providers)
            providers.reverse()
        for provider in providers:
            fcall = getattr(provider, fname, None)
            if callable(fcall):
                try:
                    r = fcall(*(args or []), **(kwargs or {}))
                    if not merge:
                        return r
                    ret = merge_dict_data(r or {}, ret or {})
                except Exception, e:
                    logger.warning("failed to resolve data (%s) from provider=%s .. skipped, error=%s" % (fcall.__name__, provider, e))
                    import sys, traceback
                    logger.warning(traceback.format_exc())

        return ret


    def resolve_queuedata(self, pandaqueue):  ## high level API
        """
            Resolve final full queue data details

            :param pandaqueue: name of PandaQueue
            :return: `QueueData` object
        """

        cache = self.queuesconf

        if pandaqueue not in cache: # not found in cache: do load and initialize data

            # the order of providers makes the priority
            r = self._resolve_data(self.whoami(), providers=(self.jobinfo, self.confinfo, self.extinfo), args=[pandaqueue],
                                   kwargs={'schedconf_priority':self.resolve_schedconf_sources()},
                                   merge=True)
            queuedata = r.get(pandaqueue)
            if queuedata:
                cache[pandaqueue] = QueueData(queuedata)

        return cache.get(pandaqueue)


    def resolve_storage_data(self, ddmendpoints=[]):  ## high level API
        """
            :return: dict of DDMEndpoint settings by DDMEndpoint name as a key
        """

        if isinstance(ddmendpoints, basestring):
            ddmendpoints = [ddmendpoints]

        cache = self.storageconf

        miss_objs = set(ddmendpoints) - set(cache)
        if miss_objs: # not found in cache: do load and initialize data
            # the order of providers makes the priority
            r = self._resolve_data(self.whoami(), providers=(self.jobinfo, self.confinfo, self.extinfo),
                                   args=[ddmendpoints], merge=True)
            cache.update(r or {})

        return cache


    def resolve_schedconf_sources(self):  ## high level API
        """
            Resolve prioritized list of source names for Schedconfig data load
            Consider first Job specific settings (via `jobinfo` instance),
            then config settings of pilot instance (``)
            and failover to default config (LOCAL, CVMFS, AGIS, PANDA)
        """

        defval = ['LOCAL', 'CVMFS', 'AGIS', 'PANDA']

        # look up priority order: either from job, local config or hardcoded in the logic
        return self._resolve_data(self.whoami(), providers=(self.jobinfo, self.confinfo)) or defval


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
    #    return self._resolve_data(self.whoami(), providers=(self.jobinfo, self.confinfo, self.extinfo), args=[name])

