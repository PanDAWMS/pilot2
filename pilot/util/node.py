#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from string import find

import logging
logger = logging.getLogger(__name__)


def get_meminfo():
    """
    Return the total memory (in MB).

    :return: memory (float).
    """

    mem = 0.0
    with open("/proc/meminfo", "r") as fd:
        mems = fd.readline()
        while mems:
            if mems.upper().find("MEMTOTAL") != -1:
                try:
                    mem = float(mems.split()[1]) / 1024
                except ValueError as e:
                    logger.warning('exception caught while trying to convert meminfo: %s' % e)
                break
            mems = fd.readline()

    return mem


def get_cpuinfo():
    """
    Return the CPU frequency (in MHz).

    :return: cpu (float).
    """

    cpu = 0.0
    with open("/proc/cpuinfo", "r") as fd:
        lines = fd.readlines()
        for line in lines:
            if not find(line, "cpu MHz"):
                try:
                    cpu = float(line.split(":")[1])
                except ValueError as e:
                    logger.warning('exception caught while trying to convert cpuinfo: %s' % e)
                break

    return cpu


def get_diskspace(path):
    """
    Return remaning disk space for the disk in the given path.
    Unit is MB.

    :param path: path to disk (string).
    :return: disk space (float).
    """

    disk = 0.0
    diskpipe = os.popen("df -mP %s" % (path))  # -m = MB
    disks = diskpipe.read()
    if not diskpipe.close():
        try:
            disk = float(disks.splitlines()[1].split()[3])
        except ValueError as e:
            logger.warning('exception caught while trying to convert disk info: %s' % e)
    return disk


def collect_workernode_info(path):
    """
    Collect node information (cpu, memory and disk space).
    The disk space (in MB) is return for the disk in the given path.

    :param path: path to disk (string).
    :return: memory (float), cpu (float), disk space (float).
    """

    mem = get_meminfo()
    cpu = get_cpuinfo()
    disk = get_diskspace(path)

    return mem, cpu, disk
