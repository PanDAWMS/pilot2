"""
Pilot Config specific info provider mainly used to customize Queue, Site, etc data of Information Service
with details fetched directly from local Pilot instance configuration

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

from ..util.config import config

import logging
logger = logging.getLogger(__name__)


class PilotConfigProvider(object):
    """
        Info provider which is used to extract settings specific for local Pilot instance
        and overwrite general configuration used by Information Service
    """

    config = None ## Pilot Config instance

    def __init__(self, conf=None):
        self.config = conf or config


    def resolve_schedconf_sources(self):
        """
            Resolve prioritized list of source names to be used for SchedConfig data load
            :return: prioritized list of source names
        """

        ## FIX ME LATER
        ## an example of return data:
        ## return ['AGIS', 'LOCAL', 'CVMFS']
        ##

        return None ## Not implemented yet


    def resolve_queuedata(self, pandaqueue, **kwargs):
        """
            Resolve queue data details

            :param pandaqueue: name of PandaQueue
            :return: dict of settings for given PandaQueue as a key
        """

        data = {'maxwdir': config.Pilot.maximum_input_file_sizes,
                }

        return dict(pandaqueue=data)
