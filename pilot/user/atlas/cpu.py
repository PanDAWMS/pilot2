#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

import os

# from .utilities import get_memory_values
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def get_core_count(job):
    """
    Return the core count from ATHENA_PROC_NUMBER.

    :param job: job object.
    :return: core count (int).
    """

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
                    logger.warning("ATHENA_PROC_NUMBER is not properly set: %s (will use existing job.corecount value)" % e)
        else:
            try:
                job.corecount = int(os.environ.get('ATHENA_PROC_NUMBER'))
            except Exception:
                logger.warning("environment variable ATHENA_PROC_NUMBER is not set. corecount is not set")

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

    # something like this could be used if prmon also gave info about ncores
    # (change nprocs -> ncores and add ncores to list in utilities module, get_average_summary_dictionary_prmon())

    #summary_dictionary = get_memory_values(job.workdir, name=job.memorymonitor)
    #if summary_dictionary:
    #    if 'nprocs' in summary_dictionary["Other"]:
    #        try:
    #            job.actualcorecount = int(summary_dictionary["Other"]["nprocs"])
    #        except Exception as e:
    #            logger.warning('exception caught: %s' % e)
    #        else:
    #            job.corecounts = add_core_count(job.actualcorecount)
    #            logger.debug('current core counts list: %s' % str(job.corecounts))
    #    else:
    #        logger.debug('summary_dictionary[Other]=%s' % summary_dictionary["Other"])
    #else:
    #    logger.debug('no summary_dictionary')

    if job.pgrp:
        # for debugging
        #cmd = "ps axo pgid,psr,comm,args | grep %d" % job.pgrp
        #exit_code, stdout, stderr = execute(cmd, mute=True)
        #logger.debug('%s:\n%s\n' % (cmd, stdout))

        # ps axo pgid,psr -> 154628   8 \n 154628   9 \n 1546280 1 ..
        # sort is redundant; uniq removes any duplicate lines; wc -l gives the final count
        # awk is added to get the pgrp list only and then grep -x makes sure that false positives are removed, e.g. 1546280
        cmd = "ps axo pgid,psr | sort | grep %d | uniq | awk '{print $1}' | grep -x %d | wc -l" % (job.pgrp, job.pgrp)
        exit_code, stdout, stderr = execute(cmd, mute=True)
        logger.debug('%s: %s' % (cmd, stdout))
        try:
            job.actualcorecount = int(stdout)
        except Exception as e:
            logger.warning('failed to convert number of actual cores to int: %s' % e)
        else:
            job.corecounts = add_core_count(job.actualcorecount)  #, core_counts=job.corecounts)
            #logger.debug('current core counts list: %s' % str(job.corecounts))
    else:
        logger.debug('payload process group not set - cannot check number of cores used by payload')
