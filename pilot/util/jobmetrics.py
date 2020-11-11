#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2020

from os import environ

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
    Job metrics are highly user specific, so this function merely calls a corresponding get_job_metrics() in the
    user code. The format of the job metrics string is defined by the server. It will be reported to the server during
    updateJob.

    Example of job metrics:
    Number of events read | Number of events written | vmPeak maximum | vmPeak average | RSS average | ..
    Format: nEvents=<int> nEventsW=<int> vmPeakMax=<int> vmPeakMean=<int> RSSMean=<int> hs06=<float> shutdownTime=<int>
            cpuFactor=<float> cpuLimit=<float> diskLimit=<float> jobStart=<int> memLimit=<int> runLimit=<float>

    :param job: job object.
    :return: job metrics (string).
    """

    user = environ.get('PILOT_USER', 'generic').lower()  # TODO: replace with singleton
    try:
        job_metrics_module = __import__('pilot.user.%s.jobmetrics' % user, globals(), locals(), [user], 0)  # Python 2/3
    except AttributeError as e:
        job_metrics = None
        logger.warning('function not implemented in jobmetrics module: %s' % e)
    else:
        job_metrics = job_metrics_module.get_job_metrics(job)

    return job_metrics
