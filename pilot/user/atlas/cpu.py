#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

import os

from pilot.util.auxiliary import get_logger
from pilot.util.container import execute


def get_core_count(job):
    """
    Return the core count from ATHENA_PROC_NUMBER.

    :param job: job object.
    :return: core count (int).
    """

    log = get_logger(job.jobid)

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
                    log.warning("ATHENA_PROC_NUMBER is not properly set: %s (will use existing job.corecount value)" % e)
        else:
            try:
                job.corecount = int(os.environ.get('ATHENA_PROC_NUMBER'))
            except Exception:
                log.warning("environment variable ATHENA_PROC_NUMBER is not set. corecount is not set")

    return job.corecount


def add_core_count(corecount, core_counts=[]):
    """
    Add a core count measurement to the list of core counts.

    :param corecount: current actual core count (int).
    :param core_counts: list of core counts (list).
    :return: updated list of core counts (list).
    """

    if core_counts is None:  # protection
        core_counts = []
    core_counts.append(corecount)

    return core_counts


def set_core_counts(job):
    """
    Set the number of used cores.

    :param job: job object.
    :return:
    """

    log = get_logger(job.jobid)

    if job.pgrp:
        cmd = "ps axo pgid,psr | sort | grep %d | uniq | wc -l" % job.pgrp
        exit_code, stdout, stderr = execute(cmd, mute=True)
        log.debug('%s: %s' % (cmd, stdout))
        try:
            job.actualcorecount = int(stdout)
        except Exception as e:
            log.warning('failed to convert number of actual cores to int: %s' % e)
        else:
            # overwrite the original core count (see discussion with Tadashi, 18/8/20) and add it to the list
            job.corecount = job.actualcorecount
            job.corecounts = add_core_count(job.actualcorecount)  #, core_counts=job.corecounts)
            log.debug('current core counts list: %s' % str(job.corecounts))

    else:
        log.debug('payload process group not set - cannot check number of cores used by payload')
