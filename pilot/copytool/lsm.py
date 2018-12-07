#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import os
import logging
import errno

from .common import get_copysetup, verify_catalog_checksum, resolve_common_transfer_errors
from pilot.common.exception import StageInFailure, StageOutFailure, PilotException, ErrorCodes
from pilot.util.container import execute


logger = logging.getLogger(__name__)

require_replicas = True  ## indicate if given copytool requires input replicas to be resolved

allowed_schemas = ['srm', 'gsiftp', 'root']  # prioritized list of supported schemas for transfers by given copytool


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER
    #for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    #return True


def is_valid_for_copy_out(files):
    #for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True


def copy_in_old(files):
    """
    Tries to download the given files using lsm-get directly.

    :param files: Files to download
    :raises PilotException: StageInFailure
    """

    if not check_for_lsm(dst_in=True):
        raise StageInFailure("No LSM tools found")
    exit_code, stdout, stderr = move_all_files_in(files)
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)


def get_timeout(filesize):   ## ISOLATE ME LATER
    """ Get a proper time-out limit based on the file size """

    timeout_max = 3 * 3600  # 3 hours
    timeout_min = 300  # self.timeout

    timeout = timeout_min + int(filesize / 0.5e6)  # approx < 0.5 Mb/sec

    return min(timeout, timeout_max)


def copy_in(files, **kwargs):
    """
    Download given files using the lsm-get command.

    :param files: list of `FileSpec` objects.
    :raise: PilotException in case of controlled error.
    :return: files `FileSpec` object.
    """

    exit_code = 0
    stdout = ""
    stderr = ""

    copytools = kwargs.get('copytools') or []
    copysetup = get_copysetup(copytools, 'lsm')

    for fspec in files:
        # continue loop for files that are to be accessed directly
        if fspec.status == 'remote_io':
            continue

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        #timeout = get_timeout(fspec.filesize)
        source = fspec.turl
        destination = os.path.join(dst, fspec.lfn)

        logger.info("transferring file %s from %s to %s" % (fspec.lfn, source, destination))

        exit_code, stdout, stderr = move(source, destination, dst_in=True, copysetup=copysetup)

        if exit_code != 0:
            logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))

            error = resolve_common_transfer_errors(stderr, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            logger.warning('error=%d' % error.get('rcode'))
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        state, diagnostics = verify_catalog_checksum(fspec, destination)
        if diagnostics != "":
            raise PilotException(diagnostics, code=fspec.status_code, state=state)

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def copy_out(files, **kwargs):
    """
    Upload given files using lsm copytool.

    :param files: list of `FileSpec` objects.
    :raise: PilotException in case of controlled error.
    """

    copytools = kwargs.get('copytools') or []
    copysetup = get_copysetup(copytools, 'lsm')
    ddmconf = kwargs.get('ddmconf', None)
    if not ddmconf:
        raise PilotException("copy_out() failed to resolve ddmconf from function arguments",
                             code=ErrorCodes.STAGEOUTFAILED,
                             state='COPY_ERROR')

    for fspec in files:

        # resolve token value from fspec.ddmendpoint
        ddm = ddmconf.get(fspec.ddmendpoint)
        token = ddm.token
        if not token:
            raise PilotException("copy_out() failed to resolve token value for ddmendpoint=%s" % (fspec.ddmendpoint),
                                 code=ErrorCodes.STAGEOUTFAILED, state='COPY_ERROR')

        src = fspec.workdir or kwargs.get('workdir') or '.'
        #timeout = get_timeout(fspec.filesize)
        source = os.path.join(src, fspec.lfn)
        destination = fspec.turl

        # checksum has been calculated in the previous step - transfer_files() in api/data
        # note: pilot is handing over checksum to the command - which will/should verify it after the transfer
        checksum = "adler32:%s" % fspec.checksum.get('adler32')

        # define the command options
        opts = {'--size': fspec.filesize,
                '-t': token,
                '--checksum': checksum,
                '--guid': fspec.guid}
        opts = " ".join(["%s %s" % (k, v) for (k, v) in opts.iteritems()])

        logger.info("transferring file %s from %s to %s" % (fspec.lfn, source, destination))

        nretries = 1  # input parameter to function?
        for retry in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=False, copysetup=copysetup, options=opts)

            if exit_code != 0:
                error = resolve_common_transfer_errors(stderr, is_stagein=False)
                fspec.status = 'failed'
                fspec.status_code = error.get('exit_code')
                raise PilotException(error.get('error'), code=error.get('exit_code'), state=error.get('state'))
            else:  # all successful
                logger.info('all successful')
                break

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def copy_out_old(files):
    """
    Tries to upload the given files using lsm-put directly.

    :param files: Files to upload
    :raises PilotException: StageOutFailure
    """

    if not check_for_lsm(dst_in=False):
        raise StageOutFailure("No LSM tools found")

    exit_code, stdout, stderr = move_all_files_out(files)
    if exit_code != 0:
        # raise failure
        raise StageOutFailure(stdout)


def move_all_files_in(files, nretries=1):
    """
    Move all files.

    :param files:
    :param nretries: number of retries; sometimes there can be a timeout copying, but the next attempt may succeed
    :return: exit_code, stdout, stderr
    """

    exit_code = 0
    stdout = ""
    stderr = ""

    for entry in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}
        logger.info("transferring file %s from %s to %s" % (entry['name'], entry['source'], entry['destination']))

        source = entry['source'] + '/' + entry['name']
        destination = os.path.join(entry['destination'], entry['name'])
        for retry in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=True)

            if exit_code != 0:
                if ((exit_code != errno.ETIMEDOUT) and (exit_code != errno.ETIME)) or (retry + 1) == nretries:
                    logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))
                    return exit_code, stdout, stderr
            else:  # all successful
                break

    return exit_code, stdout, stderr


def move_all_files_out(files, nretries=1):
    """
    Move all files.

    :param files:
    :return: exit_code, stdout, stderr
    """

    exit_code = 0
    stdout = ""
    stderr = ""

    for entry in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}
        logger.info("transferring file %s from %s to %s" % (entry['name'], entry['source'], entry['destination']))

        destination = entry['destination'] + '/' + entry['name']
        source = os.path.join(entry['source'], entry['name'])
        for retry in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=False)

            if exit_code != 0:
                if ((exit_code != errno.ETIMEDOUT) and (exit_code != errno.ETIME)) or (retry + 1) == nretries:
                    logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s" % (exit_code, stdout, stderr))
                    return exit_code, stdout, stderr
            else:  # all successful
                break

    return exit_code, stdout, stderr


def move(source, destination, dst_in=True, copysetup="", options=None):
    """
    Use lsm-get or lsm-put to transfer the file.

    :param source: path to source (string).
    :param destination: path to destination (string).
    :param dst_in: True for stage-in, False for stage-out (boolean).
    :return: exit code, stdout, stderr
    """

    # copysetup = '/osg/mwt2/app/atlas_app/atlaswn/setup.sh'
    if copysetup != "":
        cmd = 'source %s;' % copysetup
    else:
        cmd = ''

    args = "%s %s" % (source, destination)
    if options:
        args = "%s %s" % (options, args)

    if dst_in:
        cmd += "lsm-get %s" % args
    else:
        cmd += "lsm-put %s" % args

    try:
        exit_code, stdout, stderr = execute(cmd)
    except Exception as e:
        if dst_in:
            exit_code = ErrorCodes.STAGEINFAILED
        else:
            exit_code = ErrorCodes.STAGEOUTFAILED
        stdout = 'exception caught: e' % e
        stderr = ''
        logger.warning(stdout)

    logger.info('exit_code=%d, stdout=%s, stderr=%s' % (exit_code, stdout, stderr))
    return exit_code, stdout, stderr


def check_for_lsm(dst_in=True):
    cmd = None
    if dst_in:
        cmd = 'which lsm-get'
    else:
        cmd = 'which lsm-put'
    exit_code, gfal_path, _ = execute(cmd)
    return exit_code == 0
