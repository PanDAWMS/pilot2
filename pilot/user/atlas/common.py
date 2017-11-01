#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

# from pilot.common.exception import PilotException
from pilot.user.atlas.setup import should_pilot_prepare_asetup, is_user_analysis_job, get_platform, get_asetup, \
    is_standard_atlas_job

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
    prepareasetup = should_pilot_prepare_asetup(job.noExecStrCnv, job.jobPars)

    # Is it a user job or not?
    userjob = is_user_analysis_job(job.trf)

    # Get the platform value
    platform = get_platform(job.cmtConfig)

    # Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    asetuppath = get_asetup(asetup=prepareasetup)
    asetupoptions = " "

    if is_standard_atlas_job():

        # Normal setup (production and user jobs)
        logger.info("preparing normal production/analysis job setup command")

    else:  # Generic, non-ATLAS specific jobs, or at least a job with undefined swRelease

        logger.info("generic job (non-ATLAS specific or with undefined swRelease)")

    return ""
