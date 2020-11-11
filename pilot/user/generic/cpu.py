#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def get_core_count(job):
    """
    Return the core count.

    :param job: job object.
    :return: core count (int).
    """

    return 0


def add_core_count(corecount, core_counts=[]):
    """
    Add a core count measurement to the list of core counts.

    :param corecount: current actual core count (int).
    :param core_counts: list of core counts (list).
    :return: updated list of core counts (list).
    """

    return core_counts.append(corecount)


def set_core_counts(job):
    """
    Set the number of used cores.

    :param job: job object.
    :return:
    """

    if job.pgrp:
        cmd = "ps axo pgid,psr | sort | grep %d | uniq | awk '{print $1}' | grep -x %d | wc -l" % (job.pgrp, job.pgrp)
        exit_code, stdout, stderr = execute(cmd, mute=True)
        logger.debug('%s: %s' % (cmd, stdout))
        try:
            job.actualcorecount = int(stdout)
        except Exception as e:
            logger.warning('failed to convert number of actual cores to int: %s' % e)
        else:
            logger.debug('set number of actual cores to: %d' % job.actualcorecount)

            # overwrite the original core count and add it to the list
            job.corecount = job.actualcorecount
            job.corecounts = add_core_count(job.actualcorecount)
            logger.debug('current core counts list: %s' % str(job.corecounts))

    else:
        logger.debug('payload process group not set - cannot check number of cores used by payload')
