#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018
# - Tomas Javurek, tomas.javurek@cern.ch, 2019
# - David Cameron, david.cameron@cern.ch, 2019

from __future__ import absolute_import  # Python 2 (2to3 complains about this)

import os
import json
import logging
from time import time
from copy import deepcopy

from .common import resolve_common_transfer_errors, verify_catalog_checksum, get_timeout
from pilot.common.exception import PilotException, StageOutFailure, ErrorCodes
from pilot.util.timer import timeout, TimedThread

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# can be disabled for Rucio if allowed to use all RSE for input
require_replicas = True    ## indicates if given copytool requires input replicas to be resolved
require_protocols = False  ## indicates if given copytool requires protocols to be resolved first for stage-out
tracing_rucio = False      ## should Rucio send the trace?


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files):
    return True  ## FIX ME LATER


def verify_stage_out(fspec):
    """
    Checks that the uploaded file is physically at the destination.
    :param fspec: file specifications
    """
    from rucio.rse import rsemanager as rsemgr
    rse_settings = rsemgr.get_rse_info(fspec.ddmendpoint)
    uploaded_file = {'name': fspec.lfn, 'scope': fspec.scope}
    logger.info('Checking file: %s' % str(fspec.lfn))
    return rsemgr.exists(rse_settings, [uploaded_file])


#@timeout(seconds=10800)
def copy_in(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :param ignore_errors: boolean, if specified then transfer failures will be ignored
        :raise: PilotException in case of controlled error
    """

    ignore_errors = kwargs.get('ignore_errors')
    trace_report = kwargs.get('trace_report')
    use_pcache = kwargs.get('use_pcache')
    #job = kwargs.get('job')
    #use_pcache = job.infosys.queuedata.use_pcache if job else False
    logger.debug('use_pcache=%s' % use_pcache)

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', None)
    for fspec in files:
        logger.info('rucio copytool, downloading file with scope:%s lfn:%s' % (str(fspec.scope), str(fspec.lfn)))
        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
        trace_report.update(url=fspec.turl if fspec.turl else fspec.surl)
        trace_report.update(catStart=time())  ## is this metric still needed? LFC catalog
        fspec.status_code = 0
        dst = fspec.workdir or kwargs.get('workdir') or '.'
        logger.info('the file will be stored in %s' % str(dst))

        trace_report_out = []
        transfer_timeout = get_timeout(fspec.filesize)
        ctimeout = transfer_timeout + 10  # give the API a chance to do the time-out first
        logger.info('overall transfer timeout=%s' % ctimeout)

        error_msg = ""
        ec = 0
        try:
            ec, trace_report_out = timeout(ctimeout, timer=TimedThread)(_stage_in_api)(dst, fspec, trace_report, trace_report_out, transfer_timeout, use_pcache)
            #_stage_in_api(dst, fspec, trace_report, trace_report_out)
        except Exception as error:
            error_msg = str(error)
            error_details = handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=True)

            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s from %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        # make sure there was no missed failure (only way to deal with this until rucio API has been fixed)
        # (using the timeout decorator prevents the trace_report_out from being updated - rucio API should return
        # the proper error immediately instead of encoding it into a dictionary)
        state_reason = None if not trace_report_out else trace_report_out[0].get('stateReason')
        if ec and state_reason and not error_msg:
            error_details = handle_rucio_error(state_reason, trace_report, trace_report_out, fspec, stagein=True)

            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s from %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        destination = os.path.join(dst, fspec.lfn)
        if os.path.exists(destination):
            state, diagnostics = verify_catalog_checksum(fspec, destination)
            if diagnostics != "" and not ignore_errors:
                trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                    timeEnd=time())
                trace_report.send()
                raise PilotException(diagnostics, code=fspec.status_code, state=state)
        else:
            diagnostics = 'file does not exist: %s (cannot verify catalog checksum)' % destination
            logger.warning(diagnostics)
            state = 'STAGEIN_ATTEMPT_FAILED'
            fspec.status_code = ErrorCodes.STAGEINFAILED
            trace_report.update(clientState=state, stateReason=diagnostics,
                                timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=fspec.status_code, state=state)

        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())

        trace_report.send()

    return files


def handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=True):
    """

    :param error_msg:
    :param trace_report:
    :param trace_report_out:
    :param fspec:
    :return:
    """

    # try to get a better error message from the traces
    error_msg_org = error_msg
    if trace_report_out:
        logger.debug('reading stateReason from trace_report_out: %s' % trace_report_out)
        error_msg = trace_report_out[0].get('stateReason', '')
        if not error_msg or error_msg == 'OK':
            logger.warning('could not extract error message from trace report - reverting to original error message')
            error_msg = error_msg_org
    else:
        logger.debug('no trace_report_out')
    logger.info('rucio returned an error: \"%s\"' % error_msg)

    error_details = resolve_common_transfer_errors(error_msg, is_stagein=stagein)
    fspec.status = 'failed'
    fspec.status_code = error_details.get('rcode')

    msg = 'STAGEIN_ATTEMPT_FAILED' if stagein else 'STAGEOUT_ATTEMPT_FAILED'
    trace_report.update(clientState=error_details.get('state', msg),
                        stateReason=error_details.get('error'), timeEnd=time())

    return error_details


def copy_in_bulk(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :param ignore_errors: boolean, if specified then transfer failures will be ignored
        :raise: PilotException in case of controlled error
    """

    #allow_direct_access = kwargs.get('allow_direct_access')
    ignore_errors = kwargs.get('ignore_errors')
    trace_common_fields = kwargs.get('trace_report')

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    dst = kwargs.get('workdir') or '.'

    # THE DOWNLOAD
    trace_report_out = []
    try:
        # transfer_timeout = get_timeout(fspec.filesize, add=10)  # give the API a chance to do the time-out first
        # timeout(transfer_timeout)(_stage_in_api)(dst, fspec, trace_report, trace_report_out)
        _stage_in_bulk(dst, files, trace_report_out, trace_common_fields)
    except Exception as error:
        error_msg = str(error)
        # Fill and sned the traces, if they are not received from Rucio, abortion of the download process
        # If there was Exception from Rucio, but still some traces returned, we continue to VALIDATION section
        if not trace_report_out:
            trace_report = deepcopy(trace_common_fields)
            localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', None)
            diagnostics = 'None of the traces received from Rucio. Response from Rucio: %s' % error_msg
            for fspec in files:
                localsite = localsite if localsite else fspec.ddmendpoint
                trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
                trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
                trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
                trace_report.update('STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics, timeEnd=time())
                trace_report.send()
            logger.error(diagnostics)
            raise PilotException(diagnostics, code=fspec.status_code, state='STAGEIN_ATTEMPT_FAILED')

    # VALIDATION AND TERMINATION
    files_done = []
    for fspec in files:

        # getting the trace for given file
        # if one trace is missing, the whould stagin gets failed
        trace_candidates = _get_trace(fspec, trace_report_out)
        trace_report = None
        diagnostics = 'unknown'
        if len(trace_candidates) == 0:
            diagnostics = 'No trace retrieved for given file.'
            logger.error('No trace retrieved for given file. %s' % fspec.lfn)
        elif len(trace_candidates) != 1:
            diagnostics = 'Too many traces for given file.'
            logger.error('Rucio returned too many traces for given file. %s' % fspec.lfn)
        else:
            trace_report = trace_candidates[0]

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        destination = os.path.join(dst, fspec.lfn)
        if os.path.exists(destination):
            state, diagnostics = verify_catalog_checksum(fspec, destination)
            if diagnostics != "" and not ignore_errors and trace_report:  # caution, validation against empty string
                trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                    timeEnd=time())
                logger.error(diagnostics)
        elif trace_report:
            diagnostics = 'file does not exist: %s (cannot verify catalog checksum)' % destination
            state = 'STAGEIN_ATTEMPT_FAILED'
            fspec.status_code = ErrorCodes.STAGEINFAILED
            trace_report.update(clientState=state, stateReason=diagnostics, timeEnd=time())
            logger.error(diagnostics)
        else:
            fspec.status_code = ErrorCodes.STAGEINFAILED

        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())
            files_done.append(fspec)

        # updating the trace and sending it
        if not trace_report:
            logger.error('An unknown error occurred when handling the traces. %s' % fspec.lfn)
            logger.warning('No trace sent!!!')
        trace_report.update(guid=fspec.guid.replace('-', ''))
        trace_report.send()

    if len(files_done) != len(files):
        raise PilotException('Not all files downloaded.', code=ErrorCodes.STAGEINFAILED, state='STAGEIN_ATTEMPT_FAILED')

    return files_done


def _get_trace(fspec, traces):
    """
    Traces returned by Rucio are not orderred the same as input files from pilot.
    This method finds the proper trace.

    :param: fspec: the file that is seeked
    :param: traces: all traces that are received by Rucio

    :return: trace_candiates that correspond to the given file
    """
    try:
        try:
            trace_candidates = list(filter(lambda t: t['filename'] == fspec.lfn and t['scope'] == fspec.scope, traces))  # Python 2
        except Exception:
            trace_candidates = list([t for t in traces if t['filename'] == fspec.lfn and t['scope'] == fspec.scope])  # Python 3
        if trace_candidates:
            return trace_candidates
        else:
            logger.warning('File does not match to any trace received from Rucio: %s %s' % (fspec.lfn, fspec.scope))
    except Exception as error:
        logger.warning('Traces from pilot and rucio could not be merged: %s' % str(error))
        return []


#@timeout(seconds=10800)
def copy_out(files, **kwargs):  # noqa: C901
    """
        Upload given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :param ignore_errors: boolean, if specified then transfer failures will be ignored
        :raise: PilotException in case of controlled error
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    summary = kwargs.pop('summary', True)
    ignore_errors = kwargs.pop('ignore_errors', False)
    trace_report = kwargs.get('trace_report')

    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', None)
    for fspec in files:
        logger.info('rucio copytool, uploading file with scope: %s and lfn: %s' % (str(fspec.scope), str(fspec.lfn)))
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint)
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset, url=fspec.surl, filesize=fspec.filesize)
        trace_report.update(catStart=time(), filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        fspec.status_code = 0

        summary_file_path = None
        cwd = fspec.workdir or kwargs.get('workdir') or '.'
        if summary:
            summary_file_path = os.path.join(cwd, 'rucio_upload.json')

        logger.info('the file will be uploaded to %s' % str(fspec.ddmendpoint))
        trace_report_out = []
        transfer_timeout = get_timeout(fspec.filesize)
        ctimeout = transfer_timeout + 10  # give the API a chance to do the time-out first
        logger.info('overall transfer timeout=%s' % ctimeout)

        error_msg = ""
        ec = 0
        try:
            ec, trace_report_out = timeout(ctimeout, TimedThread)(_stage_out_api)(fspec, summary_file_path, trace_report, trace_report_out, transfer_timeout)
            #_stage_out_api(fspec, summary_file_path, trace_report, trace_report_out)
        except PilotException as error:
            error_msg = str(error)
            error_details = handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=False)

            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s to %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))
        except Exception as error:
            error_msg = str(error)
            error_details = handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=False)

            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s to %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        # make sure there was no missed failure (only way to deal with this until rucio API has been fixed)
        # (using the timeout decorator prevents the trace_report_out from being updated - rucio API should return
        # the proper error immediately instead of encoding it into a dictionary)
        state_reason = None if not trace_report_out else trace_report_out[0].get('stateReason')
        if ec and state_reason and not error_msg:
            error_details = handle_rucio_error(state_reason, trace_report, trace_report_out, fspec, stagein=False)

            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s from %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        if summary:  # resolve final pfn (turl) from the summary JSON
            if not os.path.exists(summary_file_path):
                logger.error('Failed to resolve Rucio summary JSON, wrong path? file=%s' % summary_file_path)
            else:
                with open(summary_file_path, 'rb') as f:
                    summary_json = json.load(f)
                    dat = summary_json.get("%s:%s" % (fspec.scope, fspec.lfn)) or {}
                    fspec.turl = dat.get('pfn')
                    logger.debug('set turl=%s' % fspec.turl)
                    # quick transfer verification:
                    # the logic should be unified and moved to base layer shared for all the movers
                    adler32 = dat.get('adler32')
                    local_checksum = fspec.checksum.get('adler32')
                    if local_checksum and adler32 and local_checksum != adler32:
                        msg = 'checksum verification failed: local %s != remote %s' % \
                              (local_checksum, adler32)
                        logger.warning(msg)
                        fspec.status = 'failed'
                        fspec.status_code = ErrorCodes.PUTADMISMATCH
                        trace_report.update(clientState='AD_MISMATCH', stateReason=msg, timeEnd=time())
                        trace_report.send()
                        if not ignore_errors:
                            raise PilotException("Failed to stageout: CRC mismatched",
                                                 code=ErrorCodes.PUTADMISMATCH, state='AD_MISMATCH')
                    else:
                        if local_checksum and adler32 and local_checksum == adler32:
                            logger.info('local checksum (%s) = remote checksum (%s)' % (local_checksum, adler32))
                        else:
                            logger.warning('checksum could not be verified: local checksum (%s), remote checksum (%s)' %
                                           (str(local_checksum), str(adler32)))
        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())

        trace_report.send()

    return files


# stageIn using rucio api.
def _stage_in_api(dst, fspec, trace_report, trace_report_out, transfer_timeout, use_pcache):

    ec = 0

    # init. download client
    from rucio.client.downloadclient import DownloadClient
    download_client = DownloadClient(logger=logger)
    if use_pcache:
        download_client.check_pcache = True

    # traces are switched off
    if hasattr(download_client, 'tracing'):
        download_client.tracing = tracing_rucio

    # file specifications before the actual download
    f = {}
    f['did_scope'] = fspec.scope
    f['did_name'] = fspec.lfn
    f['did'] = '%s:%s' % (fspec.scope, fspec.lfn)
    f['rse'] = fspec.ddmendpoint
    f['base_dir'] = dst
    f['no_subdir'] = True
    if fspec.turl:
        f['pfn'] = fspec.turl

    if transfer_timeout:
        f['transfer_timeout'] = transfer_timeout
    f['connection_timeout'] = 60 * 60

    # proceed with the download
    logger.info('rucio API stage-in dictionary: %s' % f)
    trace_pattern = {}
    if trace_report:
        trace_pattern = trace_report

    # download client raises an exception if any file failed
    try:
        logger.info('*** rucio API downloading file (taking over logging) ***')
        if fspec.turl:
            result = download_client.download_pfns([f], 1, trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)
        else:
            result = download_client.download_dids([f], trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)
    except Exception as e:
        logger.warning('*** rucio API download client failed ***')
        logger.warning('caught exception: %s' % e)
        logger.debug('trace_report_out=%s' % trace_report_out)
        # only raise an exception if the error info cannot be extracted
        if not trace_report_out:
            raise e
        if not trace_report_out[0].get('stateReason'):
            raise e
        ec = -1
    else:
        logger.info('*** rucio API download client finished ***')
        logger.debug('client returned %s' % result)

    logger.debug('trace_report_out=%s' % trace_report_out)

    return ec, trace_report_out


def _stage_in_bulk(dst, files, trace_report_out=None, trace_common_fields=None):
    """
    Stage-in files in bulk using the Rucio API.

    :param dst: destination (string).
    :param files: list of fspec objects.
    :param trace_report:
    :param trace_report_out:
    :return:
    """
    # init. download client
    from rucio.client.downloadclient import DownloadClient
    download_client = DownloadClient(logger=logger)

    # traces are switched off
    if hasattr(download_client, 'tracing'):
        download_client.tracing = tracing_rucio

    # build the list of file dictionaries before calling the download function
    file_list = []

    for fspec in files:
        fspec.status_code = 0

        # file specifications before the actual download
        f = {}
        f['did_scope'] = fspec.scope
        f['did_name'] = fspec.lfn
        f['did'] = '%s:%s' % (fspec.scope, fspec.lfn)
        f['rse'] = fspec.ddmendpoint
        f['base_dir'] = fspec.workdir or dst
        f['no_subdir'] = True
        if fspec.turl:
            f['pfn'] = fspec.turl
        else:
            logger.warning('cannot perform bulk download since fspec.turl is not set (required by download_pfns()')
            # fail somehow

        if fspec.filesize:
            f['transfer_timeout'] = get_timeout(fspec.filesize)
        f['connection_timeout'] = 60 * 60

        file_list.append(f)

    # proceed with the download
    trace_pattern = trace_common_fields if trace_common_fields else {}

    # download client raises an exception if any file failed
    num_threads = len(file_list)
    logger.info('*** rucio API downloading files (taking over logging) ***')
    try:
        result = download_client.download_pfns(file_list, num_threads, trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)
    except Exception as e:
        logger.warning('*** rucio API download client failed ***')
        logger.warning('caught exception: %s' % e)
        logger.debug('trace_report_out=%s' % trace_report_out)
        # only raise an exception if the error info cannot be extracted
        if not trace_report_out:
            raise e
        if not trace_report_out[0].get('stateReason'):
            raise e
    else:
        logger.info('*** rucio API download client finished ***')
        logger.debug('client returned %s' % result)


def _stage_out_api(fspec, summary_file_path, trace_report, trace_report_out, transfer_timeout):

    ec = 0

    # init. download client
    from rucio.client.uploadclient import UploadClient
    upload_client = UploadClient(logger=logger)

    # traces are turned off
    if hasattr(upload_client, 'tracing'):
        upload_client.tracing = tracing_rucio
    if tracing_rucio:
        upload_client.trace = trace_report

    # file specifications before the upload
    f = {}
    f['path'] = fspec.surl or getattr(fspec, 'pfn', None) or os.path.join(fspec.workdir, fspec.lfn)
    f['rse'] = fspec.ddmendpoint
    f['did_scope'] = fspec.scope
    f['no_register'] = True

    if transfer_timeout:
        f['transfer_timeout'] = transfer_timeout
    f['connection_timeout'] = 60 * 60

    # if fspec.storageId and int(fspec.storageId) > 0:
    #     if fspec.turl and fspec.is_nondeterministic:
    #         f['pfn'] = fspec.turl
    # elif fspec.lfn and '.root' in fspec.lfn:
    #     f['guid'] = fspec.guid
    if fspec.lfn and '.root' in fspec.lfn:
        f['guid'] = fspec.guid

    logger.info('rucio API stage-out dictionary: %s' % f)

    # upload client raises an exception if any file failed
    try:
        logger.info('*** rucio API uploading file (taking over logging) ***')
        logger.debug('summary_file_path=%s' % summary_file_path)
        logger.debug('trace_report_out=%s' % trace_report_out)
        result = upload_client.upload([f], summary_file_path=summary_file_path, traces_copy_out=trace_report_out)
    except Exception as e:
        logger.warning('*** rucio API upload client failed ***')
        logger.warning('caught exception: %s' % e)
        import traceback
        logger.error(traceback.format_exc())
        logger.debug('trace_report_out=%s' % trace_report_out)
        if not trace_report_out:
            raise e
        if not trace_report_out[0].get('stateReason'):
            raise e
        ec = -1
    except UnboundLocalError:
        logger.warning('*** rucio API upload client failed ***')
        logger.warning('rucio still needs a bug fix of the summary in the uploadclient')
    else:
        logger.warning('*** rucio API upload client finished ***')
        logger.debug('client returned %s' % result)

    try:
        file_exists = verify_stage_out(fspec)
        logger.info('file exists at the storage: %s' % str(file_exists))
        if not file_exists:
            raise StageOutFailure('physical check after upload failed')
    except Exception as e:
        msg = 'file existence verification failed with: %s' % e
        logger.info(msg)
        raise StageOutFailure(msg)

    return ec, trace_report_out
