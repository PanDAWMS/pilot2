#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os
import re
import glob
from time import sleep

from pilot.common.errorcodes import ErrorCodes
from pilot.util.container import execute
from pilot.util.filehandling import copy

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def get_analysis_trf(transform, workdir):
    """
    Prepare to download the user analysis transform with curl.
    The function will verify the download location from a known list of hosts.

    :param transform: full trf path (url) (string).
    :param workdir: work directory (string).
    :return: exit code (int), diagnostics (string), transform_name (string)
    """

    ec = 0
    diagnostics = ""

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        search_pattern = "%s/jobO.*.tar.gz" % harvester_workdir
        logger.debug("search_pattern - %s" % search_pattern)
        jobopt_files = glob.glob(search_pattern)
        for jobopt_file in jobopt_files:
            logger.debug("jobopt_file = %s workdir = %s" % (jobopt_file, workdir))
            try:
                copy(jobopt_file, workdir)
            except Exception as e:
                logger.error("could not copy file %s to %s : %s" % (jobopt_file, workdir, e))

    if '/' in transform:
        transform_name = transform.split('/')[-1]
    else:
        logger.warning('did not detect any / in %s (using full transform name)' % transform)
        transform_name = transform

    # is the command already available? (e.g. if already downloaded by a preprocess/main process step)
    if os.path.exists(os.path.join(workdir, transform_name)):
        logger.info('script %s is already available - no need to download again' % transform_name)
        return ec, diagnostics, transform_name

    original_base_url = ""

    # verify the base URL
    for base_url in get_valid_base_urls():
        if transform.startswith(base_url):
            original_base_url = base_url
            break

    if original_base_url == "":
        diagnostics = "invalid base URL: %s" % transform
        return errors.TRFDOWNLOADFAILURE, diagnostics, ""

    # try to download from the required location, if not - switch to backup
    status = False
    for base_url in get_valid_base_urls(order=original_base_url):
        trf = re.sub(original_base_url, base_url, transform)
        logger.debug("attempting to download script: %s" % trf)
        status, diagnostics = download_transform(trf, transform_name, workdir)
        if status:
            break

    if not status:
        return errors.TRFDOWNLOADFAILURE, diagnostics, ""

    logger.info("successfully downloaded script")
    path = os.path.join(workdir, transform_name)
    logger.debug("changing permission of %s to 0o755" % path)
    try:
        os.chmod(path, 0o755)  # Python 2/3
    except Exception as e:
        diagnostics = "failed to chmod %s: %s" % (transform_name, e)
        return errors.CHMODTRF, diagnostics, ""

    return ec, diagnostics, transform_name


def get_valid_base_urls(order=None):
    """
    Return a list of valid base URLs from where the user analysis transform may be downloaded from.
    If order is defined, return given item first.
    E.g. order=http://atlpan.web.cern.ch/atlpan -> ['http://atlpan.web.cern.ch/atlpan', ...]
    NOTE: the URL list may be out of date.

    :param order: order (string).
    :return: valid base URLs (list).
    """

    valid_base_urls = []
    _valid_base_urls = ["https://atlpan.web.cern.ch/atlpan",
                        "http://pandaserver.cern.ch"]

    if order:
        valid_base_urls.append(order)
        for url in _valid_base_urls:
            if url != order:
                valid_base_urls.append(url)
    else:
        valid_base_urls = _valid_base_urls

    return valid_base_urls


def download_transform(url, transform_name, workdir):
    """
    Download the transform from the given url
    :param url: download URL with path to transform (string).
    :param transform_name: trf name (string).
    :param workdir: work directory (string).
    :return:
    """

    status = False
    diagnostics = ""
    path = os.path.join(workdir, transform_name)
    cmd = 'curl -sS \"%s\" > %s' % (url, path)
    trial = 1
    max_trials = 3

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        # skip curl by setting max_trials = 0
        max_trials = 0
        source_path = os.path.join(harvester_workdir, transform_name)
        try:
            copy(source_path, path)
            status = True
        except Exception as error:
            status = False
            diagnostics = "Failed to copy file %s to %s : %s" % (source_path, path, error)
            logger.error(diagnostics)

    # try to download the trf a maximum of 3 times
    while trial <= max_trials:
        logger.info("executing command [trial %d/%d]: %s" % (trial, max_trials, cmd))

        exit_code, stdout, stderr = execute(cmd, mute=True)
        if not stdout:
            stdout = "(None)"
        if exit_code != 0:
            # Analyze exit code / output
            diagnostics = "curl command failed: %d, %s, %s" % (exit_code, stdout, stderr)
            logger.warning(diagnostics)
            if trial == max_trials:
                logger.fatal('could not download transform: %s' % stdout)
                status = False
                break
            else:
                logger.info("will try again after 60 s")
                sleep(60)
        else:
            logger.info("curl command returned: %s" % stdout)
            status = True
            break
        trial += 1

    return status, diagnostics
