#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os
from string import find

from pilot.util.container import execute
from pilot.util.disk import disk_usage
from pilot.util.filehandling import dump
from pilot.info import infosys

import logging
logger = logging.getLogger(__name__)


def get_local_disk_space(path):
    """
    Return remaning disk space for the disk in the given path.
    Unit is MB.

    :param path: path to disk (string). Can be None, if call to collect_workernode_info() doesn't specify it.
    :return: disk space (float).
    """

    if not path:
        return None

    disk = 0.0
    # -mP = blocks of 1024*1024 (MB) and POSIX format
    diskpipe = os.popen("df -mP %s" % (path))
    disks = diskpipe.read()
    if not diskpipe.close():
        try:
            disk = float(disks.splitlines()[1].split()[3])
        except ValueError as e:
            logger.warning('exception caught while trying to convert disk info: %s' % e)

    return disk


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
                    mem = float(mems.split()[1]) / 1024  # value listed by command as kB, convert to MB
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
                break  # command info is the same for all cores, so break here

    return cpu


def collect_workernode_info(path=None):
    """
    Collect node information (cpu, memory and disk space).
    The disk space (in MB) is return for the disk in the given path.

    :param path: path to disk (string).
    :return: memory (float), cpu (float), disk space (float).
    """

    mem = get_meminfo()
    cpu = get_cpuinfo()
    disk = get_local_disk_space(path)

    return mem, cpu, disk


def get_disk_space(queuedata):
    """
    Get the disk space from the queuedata that should be available for running the job;
    either what is actually locally available or the allowed size determined by the site (value from queuedata). This
    value is only to be used internally by the job dispatcher.

    :param queuedata: infosys object.
    :return: disk space that should be available for running the job (int).
    """

    # --- non Job related queue data
    # jobinfo provider is required to consider overwriteAGIS data coming from Job
    _maxinputsize = infosys.queuedata.maxwdir
    logger.debug("resolved value from global infosys.queuedata instance: infosys.queuedata.maxwdir=%s B" % _maxinputsize)
    _maxinputsize = queuedata.maxwdir
    logger.debug("resolved value: queuedata.maxwdir=%s B" % _maxinputsize)

    try:
        du = disk_usage(os.path.abspath("."))
        _diskspace = int(du[2] / (1024 * 1024))  # need to convert from B to MB
    except ValueError as e:
        logger.warning("failed to extract disk space: %s (will use schedconfig default)" % e)
        _diskspace = _maxinputsize
    else:
        logger.info("available WN disk space: %d MB" % (_diskspace))

    _diskspace = min(_diskspace, _maxinputsize)
    logger.info("sending disk space %d MB to dispatcher" % (_diskspace))

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


def is_virtual_machine():
    """
    Are we running in a virtual machine?
    If we are running inside a VM, then linux will put 'hypervisor' in cpuinfo. This function looks for the presence
    of that.

    :return: boolean.
    """

    status = False

    # look for 'hypervisor' in cpuinfo
    with open("/proc/cpuinfo", "r") as fd:
        lines = fd.readlines()
        for line in lines:
            if "hypervisor" in line:
                status = True
                break

    return status


def display_architecture_info():
    """
    Display OS/architecture information.
    The function attempts to use the lsb_release -a command if available. If that is not available,
    it will dump the contents of

    :return:
    """

    logger.info("architecture information:")

    exit_code, stdout, stderr = execute("lsb_release -a")
    if "Command not found" in stdout or "Command not found" in stderr:
        # Dump standard architecture info files if available
        dump("/etc/lsb-release")
        dump("/etc/SuSE-release")
        dump("/etc/redhat-release")
        dump("/etc/debian_version")
        dump("/etc/issue")
        dump("$MACHTYPE", cmd="echo")
    else:
        logger.info("\n%s" % stdout)
