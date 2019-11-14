#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019

import os
import pipes
import re
# for user container test: import urllib

from pilot.user.atlas.setup import get_asetup
from pilot.user.atlas.setup import get_file_system_root_path
from pilot.info import InfoService, infosys
from pilot.util.auxiliary import get_logger
from pilot.util.config import config
from pilot.util.filehandling import write_file
# import pilot.info.infoservice as infosys

import logging
logger = logging.getLogger(__name__)


def do_use_container(**kwargs):
    """
    Decide whether to use a container or not.

    :param kwargs: dictionary of key-word arguments.
    :return: True if function has decided that a container should be used, False otherwise (boolean).
    """

    # to force no container use: return False
    use_container = False

    job = kwargs.get('job', False)
    copytool = kwargs.get('copytool', False)
    if job:
        # for user jobs, TRF option --containerImage must have been used, ie imagename must be set
        if job.is_analysis() and job.imagename:
            use_container = False
        elif not (job.platform or job.alrbuserplatform):
            use_container = False
        else:
            queuedata = job.infosys.queuedata
            container_name = queuedata.container_type.get("pilot")
            if container_name == 'singularity':
                use_container = True
    elif copytool:
        # override for copytools - use a container for stage-in/out
        use_container = True

    return use_container


def wrapper(executable, **kwargs):
    """
    Wrapper function for any container specific usage.
    This function will be called by pilot.util.container.execute() and prepends the executable with a container command.

    :param executable: command to be executed (string).
    :param kwargs: dictionary of key-word arguments.
    :return: executable wrapped with container command (string).
    """

    workdir = kwargs.get('workdir', '.')
    pilot_home = os.environ.get('PILOT_HOME', '')
    job = kwargs.get('job', None)

    logger.info('container wrapper called')

    if workdir == '.' and pilot_home != '':
        workdir = pilot_home

    # if job.imagename (from --containerimage <image>) is set, then always use raw singularity
    if config.Container.setup_type == "ALRB" and job and not job.imagename:
        fctn = alrb_wrapper
    else:
        fctn = singularity_wrapper
    return fctn(executable, workdir, job=job)


# def use_payload_container(job):
#     pass


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

    :param platform: E.g. "x86_64-slc6" (string).
    :return: full path to grid image (string).
    """

    if not platform or platform == "":
        platform = "x86_64-slc6"
        logger.warning("using default platform=%s (cmtconfig not set)" % (platform))

    arch_and_os = extract_platform_and_os(platform)
    image = arch_and_os + ".img"
    _path = os.path.join(get_file_system_root_path(), "atlas.cern.ch/repo/containers/images/singularity")
    path = os.path.join(_path, image)
    if not os.path.exists(path):
        image = 'x86_64-centos7.img'
        logger.warning('path does not exist: %s (trying with image %s instead)' % (path, image))
        path = os.path.join(_path, image)
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
    container_type = infosys.queuedata.container_type

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


def alrb_wrapper(cmd, workdir, job=None):
    """
    Wrap the given command with the special ALRB setup for containers
    E.g. cmd = /bin/bash hello_world.sh
    ->
    export thePlatform="x86_64-slc6-gcc48-opt"
    export ALRB_CONT_RUNPAYLOAD="cmd'
    setupATLAS -c $thePlatform

    :param cmd (string): command to be executed in a container.
    :param workdir: (not used)
    :param job: job object.
    :return: prepended command with singularity execution command (string).
    """

    if not job:
        logger.warning('the ALRB wrapper did not get a job object - cannot proceed')
        return cmd

    log = get_logger(job.jobid)
    queuedata = job.infosys.queuedata

    container_name = queuedata.container_type.get("pilot")  # resolve container name for user=pilot
    if container_name == 'singularity':
        # first get the full setup, which should be removed from cmd (or ALRB setup won't work)
        _asetup = get_asetup()
        cmd = cmd.replace(_asetup, "asetup ")
        # get simplified ALRB setup (export)
        asetup = get_asetup(alrb=True)

        # Get the singularity options
        singularity_options = queuedata.container_options
        log.debug(
            "resolved singularity_options from queuedata.container_options: %s" % singularity_options)

        _cmd = asetup

        # do not include the X509_USER_PROXY in the command the container will execute
        x509 = os.environ.get('X509_USER_PROXY')
        if x509 != "":
            cmd = cmd.replace("export X509_USER_PROXY=%s;" % x509, "")
            # add it instead to the container setup command
            _cmd = "export X509_USER_PROXY=%s;" % x509 + _cmd

        if job.alrbuserplatform:
            _cmd += 'export thePlatform=\"%s\";' % job.alrbuserplatform
        elif job.platform:
            _cmd += 'export thePlatform=\"%s\";' % job.platform
        #elif '--containerImage' in job.jobparams:
        #    if job.alrbuserplatform:
        #        _cmd += 'export thePlatform=\"%s\";' % job.alrbuserplatform
        #    else:
        #        # set a default platform for user defined containers
        #        _cmd += 'export thePlatform=\"centos7\";'

        #if '--containall' not in singularity_options:
        #    singularity_options += ' --containall'
        if singularity_options != "":
            _cmd += 'export ALRB_CONT_CMDOPTS=\"%s\";' % singularity_options
        else:
            # consider using options "-c -i -p" instead of "-C". The difference is that the latter blocks all environment
            # variables by default and the former does not
            _cmd += 'export ALRB_CONT_CMDOPTS=\"$ALRB_CONT_CMDOPTS -C\";'

        # add the jobid to be used as an identifier for the payload running inside the container
        _cmd += "export PANDAID=%s;" % job.jobid

        # add TMPDIR
        cmd = "export TMPDIR=/srv;" + cmd

        # write the full payload command to a script file
        script_file = config.Container.script_file
        status = write_file(os.path.join(job.workdir, script_file), cmd, mute=False)
        if status:
            script_cmd = '. /srv/' + script_file
            _cmd += "export ALRB_CONT_RUNPAYLOAD=\'%s\';" % script_cmd
        else:
            log.warning('attempting to quote command instead')
            _cmd += 'export ALRB_CONT_RUNPAYLOAD=%s;' % pipes.quote(cmd)

        # also store the command string in the job object
        job.command = cmd

        # this should not be necessary after the extract_container_image() in JobData update
        # containerImage should have been removed already
        if '--containerImage' in job.jobparams:
            job.jobparams, container_path = remove_container_string(job.jobparams)
            if job.alrbuserplatform:
                _cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c %s' % job.alrbuserplatform
            elif container_path != "":
                _cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c %s' % container_path

                #if not job.platform:
                #    # add the default platform if not set by the job definition
                #    _cmd += ' $thePlatform'
            else:
                log.warning('failed to extract container path from %s' % job.jobparams)
                _cmd = ""
        else:
            # _cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c images'
            _cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh '
            if job.platform or job.alrbuserplatform:
                # _cmd += '+$thePlatform'
                _cmd += '-c $thePlatform'

        _cmd = _cmd.replace('  ', ' ')
        cmd = _cmd

        log.info("Updated command: %s" % cmd)
    else:
        log.warning('container %s not supported' % container_name)

    return cmd


## DEPRECATED, remove after verification with user container job
def remove_container_string(job_params):
    """ Retrieve the container string from the job parameters """

    pattern = r" \'?\-\-containerImage\=?\ ?([\S]+)\ ?\'?"
    compiled_pattern = re.compile(pattern)

    # remove any present ' around the option as well
    job_params = re.sub(r'\'\ \'', ' ', job_params)

    # extract the container path
    found = re.findall(compiled_pattern, job_params)
    container_path = found[0] if len(found) > 0 else ""

    # Remove the pattern and update the job parameters
    job_params = re.sub(pattern, ' ', job_params)

    return job_params, container_path


def singularity_wrapper(cmd, workdir, job=None):
    """
    Prepend the given command with the singularity execution command
    E.g. cmd = /bin/bash hello_world.sh
    -> singularity_command = singularity exec -B <bindmountsfromcatchall> <img> /bin/bash hello_world.sh
    singularity exec -B <bindmountsfromcatchall>  /cvmfs/atlas.cern.ch/repo/images/singularity/x86_64-slc6.img <script>
    Note: if the job object is not set, then it is assumed that the middleware container is to be used.

    :param cmd: command to be prepended (string).
    :param workdir: explicit work directory where the command should be executed (needs to be set for Singularity) (string).
    :param job: job object.
    :return: prepended command with singularity execution command (string).
    """

    if job:
        queuedata = job.infosys.queuedata
    else:
        infoservice = InfoService()
        infoservice.init(os.environ.get('PILOT_SITENAME'), infosys.confinfo, infosys.extinfo)
        queuedata = infoservice.queuedata

    container_name = queuedata.container_type.get("pilot")  # resolve container name for user=pilot
    logger.debug("resolved container_name from queuedata.contaner_type: %s" % container_name)

    if container_name == 'singularity':
        logger.info("singularity has been requested")

        # Get the singularity options
        singularity_options = queuedata.container_options
        if singularity_options != "":
            singularity_options += ","
        else:
            singularity_options = "-B "
        singularity_options += "/cvmfs,${workdir},/home"
        logger.debug("using singularity_options: %s" % singularity_options)

        # Get the image path
        if job:
            image_path = job.imagename or get_grid_image_for_singularity(job.platform)
        else:
            image_path = config.Container.middleware_container

        # Does the image exist?
        if image_path:
            # Prepend it to the given command
            cmd = "export workdir=" + workdir + "; singularity --verbose exec " + singularity_options + " " + image_path + \
                  " /bin/bash -c " + pipes.quote("cd $workdir;pwd;%s" % cmd)

            # for testing user containers
            # singularity_options = "-B $PWD:/data --pwd / "
            # singularity_cmd = "singularity exec " + singularity_options + image_path
            # cmd = re.sub(r'-p "([A-Za-z0-9.%/]+)"', r'-p "%s\1"' % urllib.pathname2url(singularity_cmd), cmd)
        else:
            logger.warning("singularity options found but image does not exist")

        logger.info("updated command: %s" % cmd)

    return cmd
