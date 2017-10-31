#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import re

from pilot.util.information import get_container_options

import logging
logger = logging.getLogger(__name__)


def extract_container_options():
    """ Extract any singularity options from catchall """

    # e.g. catchall = "somestuff singularity_options=\'-B /etc/grid-security/certificates,/var/spool/slurmd,/cvmfs,/ceph/grid,/data0,/sys/fs/cgroup\'"
    #catchall = "singularity_options=\'-B /etc/grid-security/certificates,/cvmfs,${workdir} --contain\'" #readpar("catchall")

    # ${workdir} should be there, otherwise the pilot cannot add the current workdir
    # if not there, add it

    # First try with reading new parameters from schedconfig
    container_options = get_container_options()
    if container_options == "":
        logger.warning("container_options either does not exist in queuedata or is empty, trying with catchall instead")
        catchall = get_catchall()
        # E.g. catchall = "singularity_options=\'-B /etc/grid-security/certificates,/cvmfs,${workdir} --contain\'"

        pattern = re.compile(r"singularity\_options\=\'?\"?(.+)\'?\"?")
        found = re.findall(pattern, catchall)
        if len(found) > 0:
            container_options = found[0]

    if container_options != "":
        if container_options.endswith("'") or container_options.endswith('"'):
            container_options = container_options[:-1]
        # add the workdir if missing
        if not "${workdir}" in container_options and " --contain" in container_options:
            container_options = container_options.replace(" --contain", ",${workdir} --contain")
            logger.info("Note: added missing ${workdir} to singularity_options")

    return container_options
