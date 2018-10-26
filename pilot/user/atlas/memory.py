#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from .utilities import get_memory_values
from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import get_logger, set_pilot_state
from pilot.util.processes import kill_processes

errors = ErrorCodes()


def allow_memory_usage_verifications():
    """
    Should memory usage verifications be performed?

    :return: boolean.
    """

    return True


def memory_usage(job):
    """
    Perform memory usage verification.

    :param job: job object
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    log = get_logger(job.jobid)

    # Get the maxPSS value from the memory monitor
    summary_dictionary = get_memory_values(job.workdir)

    maxdict = summary_dictionary.get('Max', {})
    maxpss_int = maxdict.get('maxPSS', -1)

    # Only proceed if values are set
    if maxpss_int != -1:
        maxrss = job.infosys.queuedata.maxrss

        if maxrss:
            try:
                maxrss_int = 2 * int(maxrss) * 1024  # Convert to int and kB
            except Exception as e:
                log.warning("unexpected value for maxRSS: %s" % e)
            else:
                # Compare the maxRSS with the maxPSS from memory monitor
                if maxrss_int > 0:
                    if maxpss_int > 0:
                        if maxpss_int > maxrss_int:
                            diagnostics = "job has exceeded the memory limit %d kB > %d kB (2 * queuedata.maxrss)" % \
                                          (maxpss_int, maxrss_int)
                            log.warning(diagnostics)

                            # Create a lockfile to let RunJob know that it should not restart the memory monitor after it has been killed
                            #pUtil.createLockFile(False, self.__env['jobDic'][k][1].workdir, lockfile="MEMORYEXCEEDED")

                            # Kill the job
                            kill_processes(job.pid)
                            set_pilot_state(job=job, state="failed")
                            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXCEEDMAXMEM)
                        else:
                            log.info("max memory (maxPSS) used by the payload is within the allowed limit: "
                                     "%d B (2 * maxRSS = %d B)" % (maxpss_int, maxrss_int))
                    else:
                        log.warning("unpected MemoryMonitor maxPSS value: %d" % (maxpss_int))
        else:
            if maxrss == 0 or maxrss == "0":
                log.info("queuedata.maxrss set to 0 (no memory checks will be done)")
            else:
                log.warning("queuedata.maxrss is not set")

    return exit_code, diagnostics
