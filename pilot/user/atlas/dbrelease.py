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
import tarfile

from pilot.common.exception import FileHandlingFailure, PilotException
from pilot.util.filehandling import write_file, mkdirs, rmdirs

import logging
logger = logging.getLogger(__name__)


def extract_version(name):
    """
    Try to extract the version from the DBRelease string.

    :param name: DBRelease (string).
    :return: version (string).
    """
    version = ""

    re_v = re.compile(r'DBRelease-(\d+\.\d+\.\d+)\.tar\.gz')  # Python 3 (added r)
    v = re_v.search(name)
    if v:
        version = v.group(1)
    else:
        re_v = re.compile(r'DBRelease-(\d+\.\d+\.\d+\.\d+)\.tar\.gz')  # Python 3 (added r)
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

    path = os.path.expandvars('$VO_ATLAS_SW_DIR/database/DBRelease') if 'VO_ATLAS_SW_DIR' in os.environ else os.path.expandvars('$OSG_APP/database/DBRelease')
    if path == "" or path.startswith('OSG_APP'):
        logger.warning("note: the DBRelease database directory is not available (will not attempt to skip DBRelease stage-in)")
    else:
        if os.path.exists(path):
            logger.info("local DBRelease path verified: %s (will attempt to skip DBRelease stage-in)" % path)
        else:
            logger.warning("note: local DBRelease path does not exist: %s (will not attempt to skip DBRelease stage-in)" % path)

    return path


def is_dbrelease_available(version):
    """
    Check whether a given DBRelease file is already available.

    :param version: DBRelease version (string).
    :return: Boolean (True is DBRelease is locally available).
    """

    status = False

    # do not proceed if
    if 'ATLAS_DBREL_DWNLD' in os.environ:
        logger.info("ATLAS_DBREL_DWNLD is set: do not skip DBRelease stage-in")
        return status

    # get the local path to the DBRelease directory
    path = get_dbrelease_dir()

    if path != "" and os.path.exists(path):
        # get the list of available DBRelease directories
        dir_list = os.listdir(path)

        # is the required DBRelease version available?
        if dir_list:
            if version in dir_list:
                logger.info("found version %s in path %s (%d releases found)" % (version, path, len(dir_list)))
                status = True
            else:
                logger.warning("did not find version %s in path %s (%d releases found)" % (version, path, len(dir_list)))
        else:
            logger.warning("empty DBRelease directory list: %s" % path)
    else:
        logger.warning('no such DBRelease path: %s' % path)

    return status


def create_setup_file(version, path):
    """
    Create the DBRelease setup file.

    :param version: DBRelease version (string).
    :param path: path to local DBReleases (string).
    :return: Boolean (True if DBRelease setup file was successfully created).
    """

    status = False

    # get the DBRelease directory
    d = get_dbrelease_dir()
    if d != "" and version != "":
        # create the python code string to be written to file
        txt = "import os\n"
        txt += "os.environ['DBRELEASE'] = '%s'\n" % version
        txt += "os.environ['DATAPATH'] = '%s/%s:' + os.environ['DATAPATH']\n" % (d, version)
        txt += "os.environ['DBRELEASE_REQUIRED'] = '%s'\n" % version
        txt += "os.environ['DBRELEASE_REQUESTED'] = '%s'\n" % version
        txt += "os.environ['CORAL_DBLOOKUP_PATH'] = '%s/%s/XMLConfig'\n" % (d, version)

        try:
            status = write_file(path, txt)
        except FileHandlingFailure as e:
            logger.warning('failed to create DBRelease setup file: %s' % e)
        else:
            logger.info("Created setup file with the following content:.................................\n%s" % txt)
            logger.info("...............................................................................")
    else:
        logger.warning('failed to create %s for DBRelease version=%s and directory=%s' % (path, version, d))

    return status


def create_dbrelease(version, path):
    """
    Create the DBRelease file only containing a setup file.

    :param version: DBRelease version (string).
    :param path: path to DBRelease (string).
    :return: Boolean (True is DBRelease file was successfully created).
    """

    status = False

    # create the DBRelease and version directories
    dbrelease_path = os.path.join(path, 'DBRelease')
    _path = os.path.join(dbrelease_path, version)
    try:
        mkdirs(_path, chmod=None)
    except PilotException as e:
        logger.warning('failed to create directories for DBRelease: %s' % e)
    else:
        logger.debug('created directories: %s' % _path)

        # create the setup file in the DBRelease directory
        version_path = os.path.join(dbrelease_path, version)
        setup_filename = "setup.py"
        _path = os.path.join(version_path, setup_filename)
        if create_setup_file(version, _path):
            logger.info("created DBRelease setup file: %s" % _path)

            # now create a new DBRelease tarball
            filename = os.path.join(path, "DBRelease-%s.tar.gz" % version)
            logger.info("creating file: %s" % filename)
            try:
                tar = tarfile.open(filename, "w:gz")
            except Exception as e:
                logger.warning("could not create DBRelease tar file: %s" % e)
            else:
                if tar:
                    # add the setup file to the tar file
                    tar.add("%s/DBRelease/%s/%s" % (path, version, setup_filename))

                    # create the symbolic link DBRelease/current ->  12.2.1
                    try:
                        _link = os.path.join(path, "DBRelease/current")
                        os.symlink(version, _link)
                    except Exception as e:
                        logger.warning("failed to create symbolic link %s: %s" % (_link, e))
                    else:
                        logger.warning("created symbolic link: %s" % _link)

                        # add the symbolic link to the tar file
                        tar.add(_link)

                        # done with the tar archive
                        tar.close()

                        logger.info("created new DBRelease tar file: %s" % filename)
                        status = True
                else:
                    logger.warning("failed to open DBRelease tar file")

            # clean up
            if rmdirs(dbrelease_path):
                logger.debug("cleaned up directories in path: %s" % dbrelease_path)
        else:
            logger.warning("failed to create DBRelease setup file")
            if rmdirs(dbrelease_path):
                logger.debug("cleaned up directories in path: %s" % dbrelease_path)

    return status
