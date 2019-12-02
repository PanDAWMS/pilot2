# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019


"""
Job specific info provider mainly used to customize Queue, Site, etc data of Information Service
with details fetched directly from Job instance

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import logging
logger = logging.getLogger(__name__)


class JobInfoProvider(object):
    """
        Job info provider which is used to extract settings specific for given Job
        and overwrite general configuration used by Information Service
    """

    job = None  ## Job instance

    def __init__(self, job):
        self.job = job

    def resolve_schedconf_sources(self):
        """
            Resolve Job specific prioritized list of source names to be used for SchedConfig data load
            :return: prioritized list of source names
        """

        ## FIX ME LATER
        ## quick stub implementation: extract later from jobParams, e.g. from overwriteAGISData..
        ## an example of return data:
        ## return ['AGIS', 'LOCAL', 'CVMFS']
        ##

        return None  ## Not implemented yet

    def resolve_queuedata(self, pandaqueue, **kwargs):
        """
            Resolve Job specific settings for queue data (overwriteQueueData)
            :return: dict of settings for given PandaQueue as a key
        """

        # use following keys from job definition
        # keys format: [(inputkey, outputkey), inputkey2]
        # outputkey is the name of external source attribute
        keys = [('platform', 'cmtconfig')]

        data = {}
        for key in keys:
            if not isinstance(key, (list, tuple)):
                key = [key, key]
            ikey = key[0]
            okey = key[1] if len(key) > 1 else key[0]
            val = getattr(self.job, ikey)
            if val:  # ignore empty or zero values -- FIX ME LATER for integers later if need
                data[okey] = val

        data.update(self.job.overwrite_queuedata)  ## use job.overwrite_queuedata as a master source

        logger.info('queuedata: following keys will be overwritten by Job values: %s' % data)

        return {pandaqueue: data}

    def resolve_storage_data(self, ddmendpoints=[], **kwargs):
        """
            Resolve Job specific settings for storage data (including data passed via --overwriteStorageData)
            :return: dict of settings for requested DDMEndpoints with ddmendpoin as a key
        """

        data = {}

        ## use job.overwrite_storagedata as a master source
        master_data = self.job.overwrite_storagedata or {}
        try:
            data.update((k, v) for k, v in master_data.iteritems() if k in set(ddmendpoints or master_data) & set(master_data))  # Python 2
        except Exception:
            data.update((k, v) for k, v in list(master_data.items()) if k in set(ddmendpoints or master_data) & set(master_data))  # Python 3

        if data:
            logger.info('storagedata: following data extracted from Job definition will be used: %s' % data)

        return data
