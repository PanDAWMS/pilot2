#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

import os
import re

import logging
logger = logging.getLogger(__name__)


def extract_version(name):
    """
    Try to extract the version from the DBRelease string.

    :param name: DBRelease (string).
    :return: version (string).
    """
    version = ""

    re_v = re.compile('DBRelease-(\d+\.\d+\.\d+)\.tar\.gz')
    v = re_v.search(name)
    if v:
        version = v.group(1)
    else:
        re_v = re.compile('DBRelease-(\d+\.\d+\.\d+\.\d+)\.tar\.gz')
        v = re_v.search(name)
        if v:
            version = v.group(1)

    return version


def get_dbrelease_version(jobpars):
    """
    Get the DBRelease version from the job parameters.

    :param jobpars: job parameters (string).
    :return: DBRelease version (string).
    """

    return extract_version(jobpars)


def get_dbrelease_dir():
    """
    Return the proper DBRelease directory

    :return: path to DBRelease (string).
    """

    path = ""
    if os.environ.has_key('VO_ATLAS_SW_DIR'):
        path = os.path.expandvars('$VO_ATLAS_SW_DIR/database/DBRelease')
    else:
        path = os.path.expandvars('$OSG_APP/database/DBRelease')
    if path == "" or path.startswith('OSG_APP'):
        logger.warning("note: the DBRelease database directory is not available (will not attempt to skip DBRelease stage-in)")
    else:
        if os.path.exists(path):
            logger.info("local DBRelease path verified: %s (will attempt to skip DBRelease stage-in)" % path)
        else:
            logger.warning("note: local DBRelease path does not exist: %s (will not attempt to skip DBRelease stage-in)" % path)

    return path
