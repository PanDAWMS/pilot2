#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os

from pilot.util.disk import disk_usage
from pilot.util.parameters import get_maximum_input_sizes

import logging
logger = logging.getLogger(__name__)


def collect_workernode_info():
    """
    Collect worker node information (cpu and memory).

    :return: mem (float), cpu (float)
    """

    mem = 0.0
    cpu = 0.0

    try:
        with open("/proc/meminfo", "r") as fd:
            mems = fd.readline()
            while mems:
                if "MEMTOTAL" in mems.upper():
                    mem = float(mems.split()[1]) / 1024
                    break
                mems = fd.readline()
    except IOError as e:
        logger.warning("failed to read /proc/meminfo: %s" % e)

    try:
        with open("/proc/cpuinfo", "r") as fd:
            lines = fd.readlines()
            for line in lines:
                if "cpu MHz" in line:
                    cpu = float(line.split(":")[1])
                    break
    except IOError as e:
        logger.warning("failed to read /proc/cpuinfo: %s" % e)

    return mem, cpu


def get_disk_space(queuedata):
    """

    Return the amound of disk space that should be available for running the job, either what is actually locally
    available or the allowed size determined by the site (value from queuedata). This value is only to be used
    internally by the job dispatcher.

    :param queuedata: (better to have a file based queuedata a la pilot 1 or some singleton memory resident object?)
    :return: disk space that should be available for running the job
    """

    _maxinputsize = get_maximum_input_sizes(queuedata)
    try:
        du = disk_usage(os.path.abspath("."))
        _diskspace = int(du[2] / (1024 * 1024))  # need to convert from B to MB
    except ValueError, e:
        logger.warning("Failed to extract disk space: %s (will use schedconfig default)" % e)
        _diskspace = _maxinputsize
    else:
        logger.info("Available WN disk space: %d MB" % (_diskspace))

    _diskspace = min(_diskspace, _maxinputsize)
    logger.info("Sending disk space %d MB to dispatcher" % (_diskspace))

    return _diskspace


def get_node_name():
    """
    Return the local node name.

    :return: node name (string)
    """
    if hasattr(os, 'uname'):
        host = os.uname()[1]
    else:
        import socket
        host = socket.gethostname()

    return get_condor_node_name(host)


def get_condor_node_name(nodename):
    """
    On a condor system, add the SlotID to the nodename

    :param nodename:
    :return:
    """

    if "_CONDOR_SLOT" in os.environ:
        nodename = "%s@%s" % (os.environ["_CONDOR_SLOT"], nodename)

    return nodename
