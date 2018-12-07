# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018

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

    config = None  # Pilot Config instance

    def __init__(self, conf=None):
        self.config = conf or config

    def resolve_schedconf_sources(self):
        """
            Resolve prioritized list of source names to be used for SchedConfig data load
            :return: prioritized list of source names
        """

        # ## FIX ME LATER
        # an example of return data:
        # return ['AGIS', 'LOCAL', 'CVMFS']

        return None  # ## Not implemented yet

    def resolve_queuedata(self, pandaqueue, **kwargs):
        """
            Resolve queue data details

            :param pandaqueue: name of PandaQueue
            :return: dict of settings for given PandaQueue as a key
        """

        import ast
        data = {
            'maxwdir_broken': self.config.Pilot.maximum_input_file_sizes,  # ## Config API is broken -- FIXME LATER
            #'container_type': 'singularity:pilot;docker:wrapper',  # ## for testing
            #'container_options': '-B /cvmfs,/scratch,/etc/grid-security --contain',  ## for testing
            #'catchall': "singularity_options='-B /cvmfs000' catchx=1",  ## for testing
            'es_stageout_gap': 601,  # in seconds, for testing: FIXME LATER,
        }

        if hasattr(self.config.Information, 'acopytools'):  ## FIX ME LATER: Config API should reimplemented/fixed later
            data['acopytools'] = ast.literal_eval(self.config.Information.acopytools)

        logger.info('queuedata: following keys will be overwritten by config values: %s' % data)

        return {pandaqueue: data}
