#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

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

    if "HPC_HPC" in readpar('catchall'):
        if job.coreCount is None:
            job.coreCount = 0
    else:
        if job.coreCount:
            # Always use the ATHENA_PROC_NUMBER first, if set
            if os.environ.has_key('ATHENA_PROC_NUMBER'):
                try:
                    job.coreCount = int(os.environ['ATHENA_PROC_NUMBER'])
                except Exception, e:
                    tolog("ATHENA_PROC_NUMBER is not properly set: %s (will use existing job.coreCount value)" % (e))
        else:
            try:
                job.coreCount = int(os.environ['ATHENA_PROC_NUMBER'])
            except:
                tolog("env ATHENA_PROC_NUMBER is not set. corecount is not set")
    coreCount = job.coreCount
    jobMetrics = ""
    if coreCount is not None and coreCount != "NULL" and coreCount != 'null':
        jobMetrics += self.jobMetric(key="coreCount", value=coreCount)

    return job_metrics
