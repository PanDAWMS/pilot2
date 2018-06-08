#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# from pilot.util.auxiliary import get_logger
from pilot.util.jobmetrics import get_job_metrics_entry
from pilot.util.processes import get_core_count

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

    # log = get_logger(job.jobid)

    job_metrics = ""

    # report core count (will also set corecount in job object)
    corecount = get_core_count(job)
    if corecount is not None and corecount != "NULL" and corecount != 'null':
        job_metrics += get_job_metrics_entry(key="coreCount", value=corecount)

    # report number of events
    if job.nevents > 0:
        job_metrics += get_job_metrics_entry(key="nEvents", value=job.nevents)
    if job.neventsw > 0:
        job_metrics += get_job_metrics_entry(key="nEventsW", value=job.neventsw)

    # if job.db_time != "":
    #     job_metrics += get_job_metrics_entry(key="dbTime", value=job.db_time)
    # if job.db_data != "":
    #     job_metrics += get_job_metrics_entry(key="dbData", value=job.db_data)

    # event service
    # if job.external_stageout_time:
    #     job_metrics += get_job_metrics_entry(key="ExStageoutTime", value=job.external_stageout_time)

    # eventservice zip file
    # if job.output_zip_name and job.output_zip_bucket_id:
    #     job_metrics += get_job_metrics_entry(key="outputZipName", value=os.path.basename(job.output_zip_name))
    #     job_metrics += get_job_metrics_entry(key="outputZipBucketID", value=job.output_zip_bucket_id)

    # report on which OS bucket the log was written to, if any
    # if job.log_bucket_id != -1:
    #     job_metrics += get_job_metrics_entry(key="logBucketID", value=job.log_bucket_id)

    # yoda
    # if job.yoda_job_metrics:
    #     for key in job.yoda_job_metrics:
    #         if key == 'startTime' or key == 'endTime':
    #             value = strftime("%Y-%m-%d %H:%M:%S", gmtime(job.yoda_mob_metrics[key]))
    #             job_metrics += get_job_metrics_entry(key=key, value=value)
    #         elif key.startswith("min") or key.startswith("max"):
    #             pass
    #         else:
    #             job_metrics += get_job_metrics_entry(key=key, value=job.yoda_job_metrics[key])

    # get the max disk space used by the payload (at the end of a job)
    #if job.state == "finished" or job.state == "failed" or job.state == "holding":
    #        max_space = getMaxWorkDirSize(path, jobId)
    #        if max_space > 0L:
    #            jobMetrics += self.addFieldToJobMetrics("workDirSize", max_space)
    #        else:
    #            tolog("Will not add max space = %d to job metrics" % (max_space))

    return job_metrics
