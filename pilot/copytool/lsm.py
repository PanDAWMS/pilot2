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
from time import time

from .common import get_copysetup, verify_catalog_checksum, resolve_common_transfer_errors  #, get_timeout
from pilot.common.exception import StageInFailure, StageOutFailure, PilotException, ErrorCodes
from pilot.util.container import execute
#from pilot.util.timer import timeout


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
    trace_report = kwargs.get('trace_report')
    #allow_direct_access = kwargs.get('allow_direct_access')
    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', None)

    for fspec in files:
        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)

        # continue loop for files that are to be accessed directly  ## TO BE DEPRECATED (anisyonk)
        #if fspec.is_directaccess(ensure_replica=False) and allow_direct_access and fspec.accessmode == 'direct':
        #    fspec.status_code = 0
        #    fspec.status = 'remote_io'
        #    trace_report.update(url=fspec.turl, clientState='FOUND_ROOT', stateReason='direct_access')
        #    trace_report.send()
        #    continue

        trace_report.update(catStart=time())

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
            trace_report.update(clientState=error.get('state') or 'STAGEIN_ATTEMPT_FAILED',
                                stateReason=error.get('error'), timeEnd=time())
            trace_report.send()
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        state, diagnostics = verify_catalog_checksum(fspec, destination)
        if diagnostics != "":
            trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=fspec.status_code, state=state)

        fspec.status_code = 0
        fspec.status = 'transferred'
        trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())
        trace_report.send()

    # for testing kill signals
    #import signal
    #os.kill(os.getpid(), signal.SIGSEGV)

    return files


def copy_out(files, **kwargs):
    """
    Upload given files using lsm copytool.

    :param files: list of `FileSpec` objects.
    :raise: PilotException in case of controlled error.
    """

    copytools = kwargs.get('copytools') or []
    copysetup = get_copysetup(copytools, 'lsm')
    trace_report = kwargs.get('trace_report')
    ddmconf = kwargs.get('ddmconf', None)
    if not ddmconf:
        raise PilotException("copy_out() failed to resolve ddmconf from function arguments",
                             code=ErrorCodes.STAGEOUTFAILED,
                             state='COPY_ERROR')

    for fspec in files:
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset, url=fspec.surl, filesize=fspec.filesize)
        trace_report.update(catStart=time(), filename=fspec.lfn, guid=fspec.guid.replace('-', ''))

        # resolve token value from fspec.ddmendpoint
        ddm = ddmconf.get(fspec.ddmendpoint)
        token = ddm.token
        if not token:
            diagnostics = "copy_out() failed to resolve token value for ddmendpoint=%s" % (fspec.ddmendpoint)
            trace_report.update(clientState='STAGEOUT_ATTEMPT_FAILED',
                                stateReason=diagnostics,
                                timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=ErrorCodes.STAGEOUTFAILED, state='COPY_ERROR')

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
        try:
            opts = " ".join(["%s %s" % (k, v) for (k, v) in opts.iteritems()])  # Python 2
        except Exception:
            opts = " ".join(["%s %s" % (k, v) for (k, v) in list(opts.items())])  # Python 3

        logger.info("transferring file %s from %s to %s" % (fspec.lfn, source, destination))

        nretries = 1  # input parameter to function?
        for retry in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=False, copysetup=copysetup, options=opts)

            if exit_code != 0:
                if stderr == "":
                    stderr = stdout
                error = resolve_common_transfer_errors(stderr, is_stagein=False)
                fspec.status = 'failed'
                fspec.status_code = error.get('exit_code')
                trace_report.update(clientState=error.get('state', None) or 'STAGEOUT_ATTEMPT_FAILED',
                                    stateReason=error.get('error', 'unknown error'),
                                    timeEnd=time())
                trace_report.send()
                raise PilotException(error.get('error'), code=error.get('exit_code'), state=error.get('state'))
            else:  # all successful
                logger.info('all successful')
                break

        fspec.status_code = 0
        fspec.status = 'transferred'
        trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())
        trace_report.send()

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


#@timeout(seconds=10800)
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
        exit_code, stdout, stderr = execute(cmd, usecontainer=False, copytool=True)  #, timeout=get_timeout(fspec.filesize))
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
