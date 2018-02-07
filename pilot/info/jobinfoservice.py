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


class JobInfoService(InfoService):
    """
        Info service: Job specific
        Job could overwrite settings provided by Info Service

        should be passed as instance to job related functions/workflow
    """

    def __init__(self, job):

        self.jobinfo = JobInfoProvider(job)
