#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2020

from .utilities import get_memory_values
from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import set_pilot_state
from pilot.util.processes import kill_processes

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def allow_memory_usage_verifications():
    """
    Should memory usage verifications be performed?

    :return: boolean.
    """

    return True


def get_ucore_scale_factor(job):
    """
    Get the correction/scale factor for SCORE/4CORE/nCORE jobs on UCORE queues/

    :param job: job object.
    :return: scale factor (int).
    """

    try:
        job_corecount = float(job.corecount)
    except Exception as e:
        logger.warning('exception caught: %s (job.corecount=%s)' % (e, str(job.corecount)))
        job_corecount = None

    try:
        schedconfig_corecount = float(job.infosys.queuedata.corecount)
    except Exception as e:
        logger.warning('exception caught: %s (job.infosys.queuedata.corecount=%s)' % (e, str(job.infosys.queuedata.corecount)))
        schedconfig_corecount = None

    if job_corecount and schedconfig_corecount:
        try:
            scale = job_corecount / schedconfig_corecount
            logger.debug('scale=%f' % scale)
        except Exception as e:
            logger.warning('exception caught: %s (using scale factor 1)' % e)
            scale = 1
    else:
        logger.debug('will use scale factor 1')
        scale = 1

    return scale


def memory_usage(job):
    """
    Perform memory usage verification.

    :param job: job object
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    # Get the maxPSS value from the memory monitor
    summary_dictionary = get_memory_values(job.workdir, name=job.memorymonitor)

    if not summary_dictionary:
        exit_code = errors.BADMEMORYMONITORJSON
        diagnostics = "Memory monitor output could not be read"
        return exit_code, diagnostics

    maxdict = summary_dictionary.get('Max', {})
    maxpss_int = maxdict.get('maxPSS', -1)

    # Only proceed if values are set
    if maxpss_int != -1:
        maxrss = job.infosys.queuedata.maxrss

        if maxrss:
            # correction for SCORE/4CORE/nCORE jobs on UCORE queues
            scale = get_ucore_scale_factor(job)
            try:
                maxrss_int = 2 * int(maxrss * scale) * 1024  # Convert to int and kB
            except Exception as e:
                logger.warning("unexpected value for maxRSS: %s" % e)
            else:
                # Compare the maxRSS with the maxPSS from memory monitor
                if maxrss_int > 0 and maxpss_int > 0:
                    if maxpss_int > maxrss_int:
                        diagnostics = "job has exceeded the memory limit %d kB > %d kB (2 * queuedata.maxrss)" % \
                                      (maxpss_int, maxrss_int)
                        logger.warning(diagnostics)

                        # Create a lockfile to let RunJob know that it should not restart the memory monitor after it has been killed
                        #pUtil.createLockFile(False, self.__env['jobDic'][k][1].workdir, lockfile="MEMORYEXCEEDED")

                        # Kill the job
                        set_pilot_state(job=job, state="failed")
                        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXCEEDMAXMEM)
                        kill_processes(job.pid)
                    else:
                        logger.info("max memory (maxPSS) used by the payload is within the allowed limit: "
                                    "%d B (2 * maxRSS = %d B)" % (maxpss_int, maxrss_int))
        else:
            if maxrss == 0 or maxrss == "0":
                logger.info("queuedata.maxrss set to 0 (no memory checks will be done)")
            else:
                logger.warning("queuedata.maxrss is not set")

    return exit_code, diagnostics
