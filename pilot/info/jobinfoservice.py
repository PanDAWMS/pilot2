# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018

"""
Job specific Info Service
It could customize/overwrite settings provided by the main Info Service

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

from .infoservice import InfoService
from .jobinfo import JobInfoProvider

import logging
logger = logging.getLogger(__name__)


class JobInfoService(InfoService):  ## TO BE DEPRECATED/REMOVED
    """
        Info service: Job specific
        Job could overwrite settings provided by Info Service

        *** KEPT for a while in repo .. most probably will be deprecated and removed soon **
    """

    def __init__(self, job):

        self.jobinfo = JobInfoProvider(job)
