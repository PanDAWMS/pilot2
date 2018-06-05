#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from pilot.util.auxiliary import get_logger

import logging
logger = logging.getLogger(__name__)


def get_job_metrics_entry(name, value):
    """
    Get a formatted job metrics entry.
    Return a a job metrics substring with the format 'name=value ' (return empty entry if value is not set).

    :param name: job metrics parameter name (string).
    :param value: job metrics parameter value (string).
    :return: job metrics entry (string).
    """

    job_metrics_entry = ""
    if value != "":
        job_metrics_entry += "%s=%s " % (name, value)

    return job_metrics_entry


def get_job_metrics(job):
    """
    Return a properly formatted job metrics string.

    Style: Number of events read | Number of events written | vmPeak maximum | vmPeak average | RSS average | ..
    Format: nEvents=<int> nEventsW=<int> vmPeakMax=<int> vmPeakMean=<int> RSSMean=<int> hs06=<float> shutdownTime=<int>
            cpuFactor=<float> cpuLimit=<float> diskLimit=<float> jobStart=<int> memLimit=<int> runLimit=<float>

    :param job: job object.
    :return: job metrics (string).
    """

    pass
