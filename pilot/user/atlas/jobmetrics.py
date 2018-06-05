#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import os

from pilot.util.auxiliary import get_logger
from pilot.util.jobmetrics import get_job_metrics_entry

import logging

logger = logging.getLogger(__name__)


def get_job_metrics(job):
    """
    Return a properly formatted job metrics string.
    The format of the job metrics string is defined by the server. It will be reported to the server during updateJob.

    Example of job metrics:
    Number of events read | Number of events written | vmPeak maximum | vmPeak average | RSS average | ..
    Format: nEvents=<int> nEventsW=<int> vmPeakMax=<int> vmPeakMean=<int> RSSMean=<int> hs06=<float> shutdownTime=<int>
            cpuFactor=<float> cpuLimit=<float> diskLimit=<float> jobStart=<int> memLimit=<int> runLimit=<float>

    :param job: job object.
    :return: job metrics (string).
    """

    log = get_logger(job.jobid)

    job_metrics = ""

    if "HPC_HPC" in job.infosys.queuedata.catchall:
        if job.corecount is None:
            job.corecount = 0
    else:
        if job.corecount:
            # Always use the ATHENA_PROC_NUMBER first, if set
            if 'ATHENA_PROC_NUMBER' in os.environ:
                try:
                    job.corecount = int(os.environ.get('ATHENA_PROC_NUMBER'))
                except Exception as e:
                    log("ATHENA_PROC_NUMBER is not properly set: %s (will use existing job.corecount value)" % e)
        else:
            try:
                job.corecount = int(os.environ.get('ATHENA_PROC_NUMBER'))
            except Exception:
                log("environment variable ATHENA_PROC_NUMBER is not set. corecount is not set")
    corecount = job.corecount

    if corecount is not None and corecount != "NULL" and corecount != 'null':
        job_metrics += get_job_metrics_entry(key="coreCount", value=corecount)

    return job_metrics
