#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from pilot.api import analytics
from pilot.util.auxiliary import get_logger
from pilot.util.jobmetrics import get_job_metrics_entry
from pilot.util.processes import get_core_count

from .common import get_db_info
from .utilities import get_memory_monitor_output_filename

import os
import logging

logger = logging.getLogger(__name__)


def get_job_metrics(job):  # noqa: C901
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

    # report core count (will also set corecount in job object)
    corecount = get_core_count(job)
    if corecount is not None and corecount != "NULL" and corecount != 'null':
        job_metrics += get_job_metrics_entry("coreCount", corecount)

    # report number of actual used cores
    if job.actualcorecount:
        job_metrics += get_job_metrics_entry("actualCoreCount", job.actualcorecount)

    # report number of events
    if job.nevents > 0:
        job_metrics += get_job_metrics_entry("nEvents", job.nevents)
    if job.neventsw > 0:
        job_metrics += get_job_metrics_entry("nEventsW", job.neventsw)

    # add metadata from job report
    if job.metadata:
        db_time, db_data = get_db_info(job.metadata)
    if job.dbtime and job.dbtime != "":
        job_metrics += get_job_metrics_entry("dbTime", job.dbtime)
    if job.dbdata and job.dbdata != "":
        job_metrics += get_job_metrics_entry("dbData", job.dbdata)

    # event service
    # if job.external_stageout_time:
    #     job_metrics += get_job_metrics_entry("ExStageoutTime", job.external_stageout_time)

    # eventservice zip file
    # if job.output_zip_name and job.output_zip_bucket_id:
    #     job_metrics += get_job_metrics_entry("outputZipName", os.path.basename(job.output_zip_name))
    #     job_metrics += get_job_metrics_entry("outputZipBucketID", job.output_zip_bucket_id)

    # report on which OS bucket the log was written to, if any
    # if job.log_bucket_id != -1:
    #     job_metrics += get_job_metrics_entry("logBucketID", job.log_bucket_id)

    # yoda
    # if job.yoda_job_metrics:
    #     for key in job.yoda_job_metrics:
    #         if key == 'startTime' or key == 'endTime':
    #             value = strftime("%Y-%m-%d %H:%M:%S", gmtime(job.yoda_mob_metrics[key]))
    #             job_metrics += get_job_metrics_entry(key, value)
    #         elif key.startswith("min") or key.startswith("max"):
    #             pass
    #         else:
    #             job_metrics += get_job_metrics_entry(key, job.yoda_job_metrics[key])

    # get the max disk space used by the payload (at the end of a job)
    if job.state == "finished" or job.state == "failed" or job.state == "holding":
        max_space = job.get_max_workdir_size()

        try:
            zero = long(0)  # Python 2
        except Exception:
            zero = 0  # Python 3

        if max_space > zero:
            job_metrics += get_job_metrics_entry("workDirSize", max_space)
        else:
            log.info("will not add max space = %d B to job metrics" % (max_space))

    # get analytics data
    path = os.path.join(job.workdir, get_memory_monitor_output_filename())
    if os.path.exists(path):
        client = analytics.Analytics()
        # do not include tails on final update
        tails = False if (job.state == "finished" or job.state == "failed" or job.state == "holding") else True
        data = client.get_fitted_data(path, tails=tails)
        slope = data.get("slope", "")
        chi2 = data.get("chi2", "")
        if slope != "":
            job_metrics += get_job_metrics_entry("leak", slope)
        if chi2 != "":
            job_metrics += get_job_metrics_entry("chi2", chi2)

    # done with job metrics, now verify the string

    # correct for potential initial and trailing space
    job_metrics = job_metrics.lstrip().rstrip()

    if job_metrics != "":
        log.debug('job metrics=\"%s\"' % (job_metrics))
    else:
        log.debug("no job metrics (all values are zero)")

    # is job_metrics within allowed size?
    if len(job_metrics) > 500:
        log.warning("job_metrics out of size (%d)" % (len(job_metrics)))

        # try to reduce the field size and remove the last entry which might be cut
        job_metrics = job_metrics[:500]
        job_metrics = " ".join(job_metrics.split(" ")[:-1])
        log.warning("job_metrics has been reduced to: %s" % (job_metrics))

    return job_metrics
