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

from pilot.common.exception import PilotException
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
            use_container = True  #False   WARNING will this change break runcontainer usage?
            logger.debug('job.is_analysis() and job.imagename -> use_container = True')
        elif not (job.platform or job.alrbuserplatform):
            use_container = False
            logger.debug('not (job.platform or job.alrbuserplatform) -> use_container = False')
        else:
            queuedata = job.infosys.queuedata
            container_name = queuedata.container_type.get("pilot")
            if container_name == 'singularity':
                use_container = True
                logger.debug('container_name == \'singularity\' -> use_container = True')
            else:
                logger.debug('else -> use_container = False')
    elif copytool:
        # override for copytools - use a container for stage-in/out
        use_container = True
        logger.debug('copytool -> use_container = False')
    else:
        logger.debug('not job -> use_container = False')

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
    if config.Container.setup_type == "ALRB":  # and job and not job.imagename:
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


def extract_atlas_setup(asetup):
    """
    Extract the asetup command from the full setup command.
    export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;
      source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;source $AtlasSetup/scripts/asetup.sh
    -> $AtlasSetup/scripts/asetup.sh

    :param asetup: full asetup command (string).
    :return: asetup command (string).
    """

    try:
        atlas_setup = asetup.split(';')[-1]  # source $AtlasSetup/scripts/asetup.sh
        atlas_setup = atlas_setup.replace('source ', '')
    except Exception as e:
        logger.debug('exception caught while extracting asetup command: %s' % e)
        atlas_setup = ''

    return atlas_setup


def extract_full_atlas_setup(cmd, atlas_setup):
    """
    Extract the full asetup (including options) from the payload setup command.
    atlas_setup is typically '$AtlasSetup/scripts/asetup.sh'.

    :param cmd: full payload setup command (string).
    :param atlas_setup: asetup command (string).
    :return: extracted full asetup command, updated full payload setup command without asetup part (string).
    """

    updated_cmds = []
    extracted_asetup = ""
    try:
        _cmd = cmd.split(';')
        for subcmd in _cmd:
            if atlas_setup in subcmd:
                extracted_asetup = subcmd
            else:
                updated_cmds.append(subcmd)
        updated_cmd = ';'.join(updated_cmds)
    except Exception as e:
        logger.warning('exception caught while extracting full atlas setup: %s' % e)
        updated_cmd = cmd
    logger.debug('updated payload setup command: %s' % updated_cmd)

    return extracted_asetup, updated_cmd


def update_alrb_setup(cmd, new_mode):
    """
    Update the ALRB setup command.
=   Add the ALRB_CONT_SETUPFILE.

    :param cmd: full ALRB setup command (string).
    :param new_mode: Boolean
    :return: updated ALRB setup command (string).
    """

    updated_cmds = []
    try:
        _cmd = cmd.split(';')
        for subcmd in _cmd:
            #if subcmd.startswith('export ATLAS_LOCAL_ROOT_BASE'):
            #    updated_cmds.append('if [ -z $ATLAS_LOCAL_ROOT_BASE ]; then ' + subcmd + '; fi')
            if subcmd.startswith('source ${ATLAS_LOCAL_ROOT_BASE}') and new_mode:
                updated_cmds.append('export ALRB_CONT_SETUPFILE="/srv/%s"' % config.Container.release_setup)
                updated_cmds.append(subcmd)
            else:
                updated_cmds.append(subcmd)
        updated_cmd = ';'.join(updated_cmds)
    except Exception as e:
        logger.warning('exception caught while extracting full atlas setup: %s' % e)
        updated_cmd = cmd
    logger.debug('updated ALRB command: %s' % updated_cmd)

    return updated_cmd


def update_for_user_proxy(_cmd, cmd):
    """
    Add the X509 user proxy to the container sub command string if set, and remove it from the main container command.

    :param _cmd:
    :param cmd:
    :return:
    """

    x509 = os.environ.get('X509_USER_PROXY')
    if x509 != "":
        # do not include the X509_USER_PROXY in the command the container will execute
        cmd = cmd.replace("export X509_USER_PROXY=%s;" % x509, "")
        # add it instead to the container setup command
        _cmd = "export X509_USER_PROXY=%s;" % x509 + _cmd

    return _cmd, cmd


def set_platform(job, new_mode, _cmd):
    """
    Set thePlatform variable and add it to the sub container command.

    :param job:
    :param new_mode:
    :param _cmd:
    :return:
    """

    if job.alrbuserplatform:
        _cmd += 'export thePlatform=\"%s\";' % job.alrbuserplatform
    elif job.imagename and new_mode:
        _cmd += 'export thePlatform=\"%s\";' % job.imagename
    elif job.platform:
        _cmd += 'export thePlatform=\"%s\";' % job.platform

    return _cmd


def add_container_options(_cmd, container_options):
    """
    Add the singularity options from queuedata to the sub container command.
    For Raythena ES jobs, replace the -C with -c -p (otherwise IPC does not work, needed by yampl).
    :param _cmd: container command (string).
    :param container_options: container options from AGIS (string).
    :return: updated container command (string).
    """

    is_raythena = config.Payload.executor_type.lower() == 'raythena'

    # Set the singularity options
    if container_options:
        # the event service payload cannot use -C/--containall since it will prevent yampl from working
        if is_raythena:
            if '-C' in container_options:
                container_options = container_options.replace('-C', '-c -p')
            if '--containall' in container_options:
                container_options = container_options.replace('--containall', '-c -p')
        if container_options:
            _cmd += 'export ALRB_CONT_CMDOPTS=\"%s\";' % container_options
    else:
        # consider using options "-c -i -p" instead of "-C". The difference is that the latter blocks all environment
        # variables by default and the former does not
        # update: skip the -i to allow IPC, otherwise yampl won't work
        if is_raythena:
            _cmd += 'export ALRB_CONT_CMDOPTS=\"$ALRB_CONT_CMDOPTS -c -p\";'
        else:
            _cmd += 'export ALRB_CONT_CMDOPTS=\"$ALRB_CONT_CMDOPTS -C\";'

    _cmd = _cmd.replace('  ', ' ')

    return _cmd


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

    new_mode = True

    container_name = queuedata.container_type.get("pilot")  # resolve container name for user=pilot
    if container_name == 'singularity':
        # first get the full setup, which should be removed from cmd (or ALRB setup won't work)
        _asetup = get_asetup()
        # get_asetup()
        # -> export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
        #     --quiet;source $AtlasSetup/scripts/asetup.sh
        logger.debug('asetup 1: %s' % _asetup)
        atlas_setup = extract_atlas_setup(_asetup)  # $AtlasSetup/scripts/asetup.sh
        if not new_mode:
            cmd = cmd.replace(_asetup, "asetup")  #else: cmd.replace(_asetup, atlas_setup)

        # get_asetup(asetup=False)
        # -> export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;

        # get simplified ALRB setup (export)
        asetup = get_asetup(alrb=True)
        # get_asetup(alrb=True)
        # -> export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;
        logger.debug('asetup 3: %s' % asetup)

        _cmd = asetup

        # add user proxy if necessary (actually it should also be removed from cmd)
        _cmd, cmd = update_for_user_proxy(_cmd, cmd)

        # set the platform info
        _cmd = set_platform(job, new_mode, _cmd)

        # add singularity options
        _cmd = add_container_options(_cmd, queuedata.container_options)

        # add the jobid to be used as an identifier for the payload running inside the container
        _cmd += "export PANDAID=%s;" % job.jobid

        # add TMPDIR
        cmd = "export TMPDIR=/srv;export GFORTRAN_TMPDIR=/srv;" + cmd
        logger.debug('cmd = %s' % cmd)
        logger.debug('queuedata.is_cmvfs = %s' % str(queuedata.is_cvmfs))

        if new_mode and queuedata.is_cvmfs:
            extracted_asetup, cmd = extract_full_atlas_setup(cmd, atlas_setup)
            # in the new mode, extracted_asetup should be written to 'my_release_setup.sh' and cmd to 'container_script.sh'
            logger.debug('command to be written to release setup file: %s' % extracted_asetup)
            status = write_file(os.path.join(job.workdir, config.Container.release_setup), extracted_asetup, mute=False)
            if not status:
                log.warning('failed to create release setup file')

        # write the full payload command to a script file
        logger.debug('command to be written to container script file: %s' % cmd)
        container_script = config.Container.container_script
        status = write_file(os.path.join(job.workdir, container_script), cmd, mute=False)
        if status:
            script_cmd = '. /srv/' + container_script
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
            else:
                log.warning('failed to extract container path from %s' % job.jobparams)
                _cmd = ""
        else:
            _cmd += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh '
            if job.platform or job.alrbuserplatform or (job.imagename and new_mode):
                _cmd += '-c $thePlatform'

        # update the ALRB setup command
        _cmd = update_alrb_setup(_cmd, new_mode and queuedata.is_cvmfs)
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


def create_stagein_container_command(workdir, cmd):
    """
    Create the stage-in container command.

    The function takes the isolated stage-in command, adds bits and pieces needed for the containerisation and stores
    it in a stagein.sh script file. It then generates the actual command that will execute the stage-in script in a
    container.

    new cmd:
      lsetup rucio davis xrootd
      old cmd
      exit $?
    write new cmd to stagein.sh script
    create container command and return it

    :param workdir: working directory where script will be stored (string).
    :param cmd: isolated stage-in command (string).
    :return: container command to be executed (string).
    """

    command = 'cd %s;' % workdir

    # add bits and pieces for the containerisation
    content = 'lsetup rucio davix xrootd\n%s\nexit $?' % cmd
    logger.debug('setup.sh content:\n%s' % content)

    # store it in setup.sh
    script_name = 'stagein.sh'
    try:
        status = write_file(os.path.join(workdir, script_name), content)
    except PilotException as e:
        raise e
    else:
        if status:
            # generate the final container command
            x509 = os.environ.get('X509_USER_PROXY', '')
            if x509:
                command += 'export X509_USER_PROXY=%s;' % x509
            pythonpath = 'export PYTHONPATH=%s:$PYTHONPATH;' % os.path.join(workdir, 'pilot2')
            #pythonpath = 'export PYTHONPATH=/cvmfs/atlas.cern.ch/repo/sw/PandaPilot/pilot2/latest:$PYTHONPATH;'
            command += 'export ALRB_CONT_RUNPAYLOAD=\"%ssource /srv/%s\";' % (pythonpath, script_name)
            command += get_asetup(alrb=True)  # export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;
            command += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c centos7'

    logger.debug('container command: %s' % command)
    return command
