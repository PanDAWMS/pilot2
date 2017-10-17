#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

from pilot.common.exception import PilotException
from pilot.user.atlas.setup import should_pilot_prepare_asetup

import logging
logger = logging.getLogger(__name__)


def get_payload_command(job):
    """
    Return the full command for execuring the payload, including the sourcing of all setup files and setting of
    environment variables.

    :param job: job object
    :return: command (string)
    """

    # Should the pilot do the asetup or do the jobPars already contain the information?
    prepareASetup = should_pilot_prepare_asetup(job.noExecStrCnv, job.jobPars)

    # Is it a user job or not?
    analysisJob = isAnalysisJob(job.trf)

    return ""