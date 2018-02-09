#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch

import os
import re

from pilot.util.information import get_container_options, get_container_type, get_catchall
from pilot.user.atlas.setup import get_file_system_root_path

import logging
logger = logging.getLogger(__name__)


def wrapper(executable, **kwargs):
    """
    Wrapper function for any container specific usage.
    This function will be called by pilot.util.container.execute() and prepends the executable with a container command.

    :param executable: command to be executed (string).
    :param kwargs:
    :return: executable wrapped with container command (string).
    """

    platform = kwargs.get('platform', '')
    workdir = kwargs.get('workdir', '.')
    pilot_home = os.environ.get('PILOT_HOME', '')
    if workdir == '.' and pilot_home != '':
        workdir = pilot_home

    return singularity_wrapper(executable, platform, workdir)


def use_payload_container(job):
    pass


def use_middleware_container():
    """
    Should middleware from container be used?
    In case middleware, i.e. the copy command for stage-in/out, should be taken from a container this function should
    return True.

    :return: True if middleware should be taken from container. False otherwise.
    """

    if get_middleware_type() == 'container':
        return True
    else:
        return False


def get_middleware_container():
    pass


def extract_container_options():
    """ Extract any singularity options from catchall """

    # e.g. catchall = "somestuff singularity_options=\'-B /etc/grid-security/certificates,/var/spool/slurmd,/cvmfs,/ceph/grid,/data0,/sys/fs/cgroup\'"
    # catchall = "singularity_options=\'-B /etc/grid-security/certificates,/cvmfs,${workdir} --contain\'" #readpar("catchall")

    # ${workdir} should be there, otherwise the pilot cannot add the current workdir
    # if not there, add it

    # First try with reading new parameters from schedconfig
    container_options = get_container_options()
    if container_options == "":
        logger.warning("container_options either does not exist in queuedata or is empty, trying with catchall instead")
        catchall = get_catchall()
        logger.info('catchall=%s'%str(catchall))
        # E.g. catchall = "singularity_options=\'-B /etc/grid-security/certificates,/cvmfs,${workdir} --contain\'"

        pattern = re.compile(r"singularity\_options\=\'?\"?(.+)\'?\"?")
        found = re.findall(pattern, catchall)
        logger.info('found=%s'%str(found))
        if len(found) > 0:
            container_options = found[0]
            logger.info('extracted from catchall: %s' % str(container_options))

    logger.info('2 container_options=%s'%str(container_options))
    if container_options and container_options != "":
        if container_options.endswith("'") or container_options.endswith('"'):
            container_options = container_options[:-1]
        # add the workdir if missing
        if "${workdir}" not in container_options and " --contain" in container_options:
            container_options = container_options.replace(" --contain", ",${workdir} --contain")
            logger.info("Note: added missing ${workdir} to singularity_options")

    logger.info('from catchall: %s' % str(container_options))
    return container_options


def extract_platform_and_os(platform):
    """
    Extract the platform and OS substring from platform

    :param platform (string): E.g. "x86_64-slc6-gcc48-opt"
    :return: extracted platform specifics (string). E.g. "x86_64-slc6". In case of failure, return the full platform
    """

    pattern = r"([A-Za-z0-9_-]+)-.+-.+"
    a = re.findall(re.compile(pattern), platform)

    if len(a) > 0:
        ret = a[0]
    else:
        logger.warning("could not extract architecture and OS substring using pattern=%s from platform=%s"
                       "(will use %s for image name)" % (pattern, platform, platform))
        ret = platform

    return ret


def get_grid_image_for_singularity(platform):
    """
    Return the full path to the singularity grid image

    :param platform (string): E.g. "x86_64-slc6"
    :return: full path to grid image (string).
    """

    if not platform or platform == "":
        platform = "x86_64-slc6"
        logger.warning("using default platform=%s (cmtconfig not set)" % (platform))

    arch_and_os = extract_platform_and_os(platform)
    image = arch_and_os + ".img"
    _path = os.path.join(get_file_system_root_path(), "atlas.cern.ch/repo/containers/images/singularity")
    logger.info('_path=%s' % str(_path))
    path = os.path.join(_path, image)
    logger.info('path=%s' % str(path))
    if not os.path.exists(path):
        image = 'x86_64-centos7.img'
        logger.warning('path does not exist: %s (trying with image %s instead)' % (path, image))
        logger.info('_path=%s' % str(_path))
        logger.info('image=%s' % str(image))
        path = os.path.join(_path, image)
        logger.info('path=%s' % str(path))
        if not os.path.exists(path):
            logger.warning('path does not exist either: %s' % path)
            path = ""

    return path


def get_middleware_type():
    """
    Return the middleware type from the container type.
    E.g. container_type = 'singularity:pilot;docker:wrapper;middleware:container'
    get_middleware_type() -> 'container', meaning that middleware should be taken from the container. The default
    is otherwise 'workernode', i.e. middleware is assumed to be present on the worker node.

    :return: middleware_type (string)
    """

    middleware_type = ""
    container_type = get_container_type()

    mw = 'middleware'
    if container_type and container_type != "" and mw in container_type:
        try:
            container_names = container_type.split(';')
            for name in container_names:
                t = name.split(':')
                if mw == t[0]:
                    middleware_type = t[1]
        except Exception as e:
            logger.warning("failed to parse the container name: %s, %s" % (container_type, e))
    else:
        # logger.warning("container middleware type not specified in queuedata")
        # no middleware type was specified, assume that middleware is present on worker node
        middleware_type = "workernode"

    return middleware_type


def get_container_name(user="pilot"):
    """
    Return the container name
    E.g. container_type = 'singularity:pilot;docker:wrapper'
    get_container_name(user='pilot') -> return 'singularity'

    :param user (string): E.g. "pilot" or "wrapper".
    :return: container name (string). E.g. "singularity"
    """

    container_name = ""
    container_type = get_container_type()

    if container_type and container_type != "" and user in container_type:
        try:
            container_names = container_type.split(';')
            for name in container_names:
                t = name.split(':')
                if user == t[1]:
                    container_name = t[0]
        except Exception as e:
            logger.warning("failed to parse the container name: %s, %s" % (container_type, e))
    else:
        logger.warning("container type not specified in queuedata")

    return container_name


def singularity_wrapper(cmd, platform, workdir):
    """
    Prepend the given command with the singularity execution command
    E.g. cmd = /bin/bash hello_world.sh
    -> singularity_command = singularity exec -B <bindmountsfromcatchall> <img> /bin/bash hello_world.sh
    singularity exec -B <bindmountsfromcatchall>  /cvmfs/atlas.cern.ch/repo/images/singularity/x86_64-slc6.img <script>

    :param cmd (string): command to be prepended.
    :param platform (string): platform specifics.
    :param workdir: explicit work directory where the command should be executed (needs to be set for Singularity).
    :return: prepended command with singularity execution command (string).
    """

    # Should a container be used?
    container_name = get_container_name()
    if container_name == 'singularity':
        logger.info("singularity has been requested")

        # Get the singularity options
        singularity_options = extract_container_options()
        if singularity_options != "" or True:
            # Get the image path
            image_path = get_grid_image_for_singularity(platform)

            logger.info('image_path=%s'%str(image_path))
            logger.info('workdir=%s'%str(workdir))
            logger.info('singularity_options=%s'%str(singularity_options))
            logger.info('image_path=%s'%str(image_path))
            logger.info('cmd=%s'%str(cmd))

            # Does the image exist?
            if image_path != '':
                # Prepend it to the given command
                cmd = "export workdir=" + workdir + "; singularity exec " + singularity_options + " " + image_path + \
                      " /bin/bash -c \'cd $workdir;pwd;" + cmd.replace("\'", "\\'").replace('\"', '\\"') + "\'"
            else:
                logger.warning("singularity options found but image does not exist")
        else:
            # Return the original command as it was
            logger.warning("no singularity options found in container_options or catchall fields")

    logger.info("Updated command: %s" % cmd)

    return cmd
