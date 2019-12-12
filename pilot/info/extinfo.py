# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

"""
Information provider from external source(s)
which is mainly used to retrive Queue, Site, etc data required for Information Service

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import os
import json
import random
from pilot.util.config import config
from .dataloader import DataLoader, merge_dict_data

import logging
logger = logging.getLogger(__name__)


class ExtInfoProvider(DataLoader):
    """
        Information provider to retrive data from external source(s)
        (e.g. AGIS, PanDA, CVMFS)
    """

    def __init__(self, cache_time=60):
        """
            :param cache_time: Default cache time in seconds
        """

        self.cache_time = cache_time

    @classmethod
    def load_schedconfig_data(self, pandaqueues=[], priority=[], cache_time=60):
        """
        Download the (AGIS-extended) data associated to PandaQueue from various sources (prioritized).
        Try to get data from CVMFS first, then AGIS or from Panda JSON sources (not implemented).

        For the moment PanDA source does not provide the full schedconfig description

        :param pandaqueues: list of PandaQueues to be loaded
        :param cache_time: Default cache time in seconds.
        :return:
        """

        pandaqueues = set(pandaqueues)

        cache_dir = config.Information.cache_dir
        if not cache_dir:
            cache_dir = os.environ.get('PILOT_HOME', '.')

        sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_schedconf.json',
                             'nretry': 1,
                             'fname': os.path.join(cache_dir, 'agis_schedconf.cvmfs.json')},
                   'AGIS': {'url': 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json'
                                   '&preset=schedconf.all&panda_queue=%s' % ','.join(pandaqueues),
                            'nretry': 3,
                            'sleep_time': lambda: 15 + random.randint(0, 30),  ## max sleep time 45 seconds between retries
                            'cache_time': 3 * 60 * 60,  # 3 hours
                            'fname': os.path.join(cache_dir, 'agis_schedconf.agis.%s.json' %
                                                  ('_'.join(sorted(pandaqueues)) or 'ALL'))},
                   'LOCAL': {'url': os.environ.get('LOCAL_AGIS_SCHEDCONF', None),
                             'nretry': 1,
                             'cache_time': 3 * 60 * 60,  # 3 hours
                             'fname': os.path.join(cache_dir, 'agis_schedconf.json')},
                   'PANDA': None  ## NOT implemented, FIX ME LATER
                   }

        priority = priority or ['LOCAL', 'CVMFS', 'AGIS', 'PANDA']

        return self.load_data(sources, priority, cache_time)

    @classmethod
    def load_queuedata(self, pandaqueue, priority=[], cache_time=60):
        """
        Download the queuedata from various sources (prioritized).
        Try to get data from PanDA, CVMFS first, then AGIS

        This function retrieves only min information of queuedata provided by PanDA cache for the moment.

        :param pandaqueue: PandaQueue name
        :param cache_time: Default cache time in seconds.
        :return:
        """

        if not pandaqueue:
            raise Exception('load_queuedata(): pandaqueue name is not specififed')

        pandaqueues = [pandaqueue]

        cache_dir = config.Information.cache_dir
        if not cache_dir:
            cache_dir = os.environ.get('PILOT_HOME', '.')

        def jsonparser_panda(c):
            dat = json.loads(c)
            if dat and isinstance(dat, dict) and 'error' in dat:
                raise Exception('response contains error, data=%s' % dat)
            return {pandaqueue: dat}

        sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_schedconf.json',
                             'nretry': 1,
                             'fname': os.path.join(cache_dir, 'agis_schedconf.cvmfs.json')},
                   'AGIS': {'url': 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json'
                                   '&preset=schedconf.all&panda_queue=%s' % ','.join(pandaqueues),
                            'nretry': 3,
                            'sleep_time': lambda: 15 + random.randint(0, 30),  # max sleep time 45 seconds between retries
                            'cache_time': 3 * 60 * 60,  # 3 hours
                            'fname': os.path.join(cache_dir, 'agis_schedconf.agis.%s.json' %
                                                  ('_'.join(sorted(pandaqueues)) or 'ALL'))},
                   'LOCAL': {'url': None,
                             'nretry': 1,
                             'cache_time': 3 * 60 * 60,  # 3 hours
                             'fname': os.path.join(cache_dir, 'queuedata.json'),
                             'parser': jsonparser_panda
                             },
                   # FIX ME LATER: move hardcoded urls to the Config?
                   'PANDA': {'url': 'http://pandaserver.cern.ch:25085/cache/schedconfig/%s.all.json' % pandaqueues[0],
                             'nretry': 3,
                             'sleep_time': lambda: 15 + random.randint(0, 30),  # max sleep time 45 seconds between retries
                             'cache_time': 3 * 60 * 60,  # 3 hours,
                             'fname': os.path.join(cache_dir, 'queuedata.json'),
                             'parser': jsonparser_panda
                             }
                   }

        priority = priority or ['LOCAL', 'PANDA', 'CVMFS', 'AGIS']

        return self.load_data(sources, priority, cache_time)

    @classmethod
    def load_storage_data(self, ddmendpoints=[], priority=[], cache_time=60):
        """
        Download DDM Storages details by given name (DDMEndpoint) from various sources (prioritized).
        Try to get data from LOCAL first, then CVMFS and AGIS

        :param pandaqueues: list of PandaQueues to be loaded
        :param cache_time: Default cache time in seconds.
        :return: dict of DDMEndpoint settings by DDMendpoint name as a key
        """

        ddmendpoints = set(ddmendpoints)

        cache_dir = config.Information.cache_dir
        if not cache_dir:
            cache_dir = os.environ.get('PILOT_HOME', '.')

        # list of sources to fetch ddmconf data from
        sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json',
                             'nretry': 1,
                             'fname': os.path.join(cache_dir, 'agis_ddmendpoints.json')},
                   'AGIS': {'url': 'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&'
                                   'state=ACTIVE&preset=dict&ddmendpoint=%s' % ','.join(ddmendpoints),
                            'nretry': 3,
                            'sleep_time': lambda: 15 + random.randint(0, 30),  ## max sleep time 45 seconds between retries
                            'cache_time': 3 * 60 * 60,  # 3 hours
                            'fname': os.path.join(cache_dir, 'agis_ddmendpoints.agis.%s.json' %
                                                  ('_'.join(sorted(ddmendpoints)) or 'ALL'))},
                   'LOCAL': {'url': None,
                             'nretry': 1,
                             'cache_time': 3 * 60 * 60,  # 3 hours
                             'fname': os.path.join(cache_dir, 'agis_ddmendpoints.json')},
                   'PANDA': None  ## NOT implemented, FIX ME LATER if need
                   }

        priority = priority or ['LOCAL', 'CVMFS', 'AGIS', 'PANDA']

        return self.load_data(sources, priority, cache_time)

    def resolve_queuedata(self, pandaqueue, schedconf_priority=None):
        """
            Resolve final full queue data details
            (primary data provided by PanDA merged with overall queue details from AGIS)

            :param pandaqueue: name of PandaQueue
            :return: dict of settings for given PandaQueue as a key
        """

        # load queuedata (min schedconfig settings)
        master_data = self.load_queuedata(pandaqueue, cache_time=self.cache_time)  ## use default priority

        # load full queue details
        r = self.load_schedconfig_data([pandaqueue], priority=schedconf_priority, cache_time=self.cache_time)

        # merge
        return merge_dict_data(r, master_data)

    def resolve_storage_data(self, ddmendpoints=[]):
        """
            Resolve final DDM Storages details by given names (DDMEndpoint)

            :param ddmendpoints: list of ddmendpoint names
            :return: dict of settings for given DDMEndpoint as a key
        """

        # load ddmconf settings
        return self.load_storage_data(ddmendpoints, cache_time=self.cache_time)  ## use default priority
