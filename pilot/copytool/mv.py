#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019
# - Tobias Wegner, tobias.wegner@cern.ch, 2018
# - David Cameron, david.cameron@cern.ch, 2018-2019

import os
import re

from pilot.common.exception import StageInFailure, StageOutFailure, ErrorCodes, PilotException
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)

require_replicas = False  # indicate if given copytool requires input replicas to be resolved


def create_output_list(files, init_dir, ddmconf):
    """
    Add files to the output list which tells ARC CE which files to upload
    """

    if not ddmconf:
        raise PilotException("copy_out() failed to resolve ddmconf from function arguments",
                             code=ErrorCodes.STAGEOUTFAILED,
                             state='COPY_ERROR')

    for fspec in files:
        arcturl = fspec.turl
        if arcturl.startswith('s3://'):
            # Use Rucio proxy to upload to OS
            arcturl = re.sub(r'^s3', 's3+rucio', arcturl)
            # Add failureallowed option so failed upload does not fail job
            rucio = 'rucio://rucio-lb-prod.cern.ch;failureallowed=yes/objectstores'
            rse = fspec.ddmendpoint
            activity = 'write'
            arcturl = '/'.join([rucio, arcturl, rse, activity])
        else:
            # Add ARC options to TURL
            checksumtype, checksum = list(fspec.checksum.items())[0]  # Python 2/3
            # resolve token value from fspec.ddmendpoint
            token = ddmconf.get(fspec.ddmendpoint).token
            if not token:
                logger.info('No space token info for %s' % fspec.ddmendpoint)
            else:
                arcturl = re.sub(r'((:\d+)/)', r'\2;autodir=no;spacetoken=%s/' % token, arcturl)
            arcturl += ':checksumtype=%s:checksumvalue=%s' % (checksumtype, checksum)

        logger.info('Adding to output.list: %s %s' % (fspec.lfn, arcturl))
        # Write output.list
        with open(os.path.join(init_dir, 'output.list'), 'a') as f:
            f.write('%s %s\n' % (fspec.lfn, arcturl))


def is_valid_for_copy_in(files):
    return True  # FIX ME LATER
    #for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    #return True


def is_valid_for_copy_out(files):
    return True  # FIX ME LATER
    #for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    #return True


def copy_in(files, copy_type="symlink", **kwargs):
    """
    Tries to download the given files using mv directly.

    :param files: list of `FileSpec` objects
    :raises PilotException: StageInFailure
    """

    # make sure direct access is not attempted (wrong queue configuration - pilot should fail job)
    allow_direct_access = kwargs.get('allow_direct_access')
    for fspec in files:
        if fspec.is_directaccess(ensure_replica=False) and allow_direct_access and fspec.accessmode == 'direct':
            fspec.status_code = ErrorCodes.BADQUEUECONFIGURATION
            raise StageInFailure("bad queue configuration - mv does not support direct access")

    if copy_type not in ["cp", "mv", "symlink"]:
        raise StageInFailure("incorrect method for copy in")

    if not kwargs.get('workdir'):
        raise StageInFailure("workdir is not specified")

    exit_code, stdout, stderr = move_all_files(files, copy_type, kwargs.get('workdir'))
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)

    return files


def copy_out(files, copy_type="mv", **kwargs):
    """
    Tries to upload the given files using mv directly.

    :param files: list of `FileSpec` objects
    :raises PilotException: StageOutFailure
    """

    if copy_type not in ["cp", "mv"]:
        raise StageOutFailure("incorrect method for copy out")

    if not kwargs.get('workdir'):
        raise StageOutFailure("Workdir is not specified")

    exit_code, stdout, stderr = move_all_files(files, copy_type, kwargs.get('workdir'))
    if exit_code != 0:
        # raise failure
        raise StageOutFailure(stdout)

    # Create output list for ARC CE if necessary
    logger.debug('init_dir for output.list=%s' % os.path.dirname(kwargs.get('workdir')))
    output_dir = kwargs.get('output_dir', '')
    if not output_dir:
        create_output_list(files, os.path.dirname(kwargs.get('workdir')), kwargs.get('ddmconf', None))

    return files


def move_all_files(files, copy_type, workdir):
    """
    Move all files.

    :param files: list of `FileSpec` objects
    :return: exit_code, stdout, stderr
    """

    exit_code = 0
    stdout = ""
    stderr = ""
    # copy_method = None

    if copy_type == "mv":
        copy_method = move
    elif copy_type == "cp":
        copy_method = copy
    elif copy_type == "symlink":
        copy_method = symlink
    else:
        return -1, "", "incorrect copy method"

    for fspec in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}

        name = fspec.lfn
        if fspec.filetype == 'input':
            # Assumes pilot runs in subdir one level down from working dir
            source = os.path.join(os.path.dirname(workdir), name)
            destination = os.path.join(workdir, name)
        else:
            source = os.path.join(workdir, name)
            destination = os.path.join(os.path.dirname(workdir), name)

        # resolve canonical path
        source = os.path.realpath(source)

        logger.info("transferring file %s from %s to %s" % (name, source, destination))

        exit_code, stdout, stderr = copy_method(source, destination)
        if exit_code != 0:
            logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))
            fspec.status = 'failed'
            if fspec.filetype == 'input':
                fspec.status_code = ErrorCodes.STAGEINFAILED
            else:
                fspec.status_code = ErrorCodes.STAGEOUTFAILED
            break
        else:
            fspec.status_code = 0
            fspec.status = 'transferred'

    return exit_code, stdout, stderr


def move(source, destination):
    """
    Tries to upload the given files using mv directly.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'mv', source, destination]
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr


def copy(source, destination):
    """
    Tries to upload the given files using xrdcp directly.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'cp', source, destination]
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr


def symlink(source, destination):
    """
    Tries to ln the given files.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'ln', '-s', source, destination]
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr
