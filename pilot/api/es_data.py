#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern,ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2019
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

import logging

from pilot.api.data import StageInClient, StageOutClient

logger = logging.getLogger(__name__)


class StageInESClient(StageInClient):

    def __init__(self, *argc, **kwargs):
        super(StageInESClient, self).__init__(*argc, **kwargs)

        self.copytool_modules.setdefault('objectstore', {'module_name': 'objectstore'})
        self.acopytools.setdefault('es_events_read', ['objectstore'])

    def prepare_sources(self, files, activities=None):
        """
            Customize/prepare source data for each entry in `files` optionally checking data for requested `activities`
            (custom StageClient could extend this logic if need)
            :param files: list of `FileSpec` objects to be processed
            :param activities: string or ordered list of activities to resolve `astorages` (optional)
            :return: None

            If storage_id is specified, replace ddmendpoint by parsing storage_id
        """

        if not self.infosys:
            self.logger.warning('infosys instance is not initialized: skip calling prepare_sources()')
            return

        for fspec in files:
            if fspec.storage_token:   ## FIX ME LATER: no need to parse each time storage_id, all this staff should be applied in FileSpec clean method
                storage_id, path_convention = fspec.get_storage_id_and_path_convention()
                if path_convention and path_convention == 1000:
                    fspec.scope = 'transient'
                if storage_id:
                    fspec.ddmendpoint = self.infosys.get_ddmendpoint(storage_id)
                logger.info("Processed file with storage id: %s" % fspec)


class StageOutESClient(StageOutClient):

    def __init__(self, *argc, **kwargs):
        super(StageOutESClient, self).__init__(*argc, **kwargs)

        self.copytool_modules.setdefault('objectstore', {'module_name': 'objectstore'})
        self.acopytools.setdefault('es_events', ['objectstore'])
