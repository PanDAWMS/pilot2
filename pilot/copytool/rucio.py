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

from __future__ import absolute_import

import os
import json
import logging
from time import time

from .common import resolve_common_transfer_errors, verify_catalog_checksum, get_timeout
from pilot.common.exception import PilotException, StageOutFailure, ErrorCodes
from pilot.util.timer import timeout

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

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', os.environ.get('DQ2_LOCAL_SITE_ID', None))
    for fspec in files:
        logger.info('rucio copytool, downloading file with scope:%s lfn:%s' % (str(fspec.scope), str(fspec.lfn)))
        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)

        trace_report.update(catStart=time())  ## is this metric still needed? LFC catalog
        fspec.status_code = 0
        dst = fspec.workdir or kwargs.get('workdir') or '.'
        logger.info('the file will be stored in %s' % str(dst))

        trace_report_out = []
        transfer_timeout = get_timeout(fspec.filesize)
        ctimeout = transfer_timeout + 10  # give the API a chance to do the time-out first
        logger.info('overall transfer timeout=%s' % ctimeout)

        try:
            timeout(ctimeout)(_stage_in_api)(dst, fspec, trace_report, trace_report_out, transfer_timeout)
            #_stage_in_api(dst, fspec, trace_report, trace_report_out)
        except Exception as error:
            error_msg = str(error)
            # Try to get a better error message from the traces
            if trace_report_out and trace_report_out[0].get('stateReason'):
                error_msg = trace_report_out[0].get('stateReason')
            logger.info('rucio returned an error: %s' % error_msg)

            error_details = resolve_common_transfer_errors(error_msg, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error_details.get('rcode')
            trace_report.update(clientState=error_details.get('state', 'STAGEIN_ATTEMPT_FAILED'),
                                stateReason=error_details.get('error'), timeEnd=time())
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


def copy_in_bulk(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :param ignore_errors: boolean, if specified then transfer failures will be ignored
        :raise: PilotException in case of controlled error
    """

    #allow_direct_access = kwargs.get('allow_direct_access')
    ignore_errors = kwargs.get('ignore_errors')
    trace_report = kwargs.get('trace_report')
    trace_reports = [trace_report]

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    dst = kwargs.get('workdir') or '.'

    trace_report_out = []
    try:
        #transfer_timeout = get_timeout(fspec.filesize, add=10)  # give the API a chance to do the time-out first
        #timeout(transfer_timeout)(_stage_in_api)(dst, fspec, trace_report, trace_report_out)
        _stage_in_api_bulk(dst, files, trace_reports, trace_report_out)
    except Exception as error:
        error_msg = str(error)
        # Try to get a better error message from the traces
        if trace_report_out and trace_report_out[0].get('stateReason'):
            error_msg = trace_report_out[0].get('stateReason')
        logger.info('rucio returned an error: %s' % error_msg)

        error_details = resolve_common_transfer_errors(error_msg, is_stagein=True)
        for fspec, trace_report in zip(files, trace_reports):
            fspec.status = 'failed'
            fspec.status_code = error_details.get('rcode')
            trace_report.update(clientState=error_details.get('state', 'STAGEIN_ATTEMPT_FAILED'),
                                stateReason=error_details.get('error'), timeEnd=time())
            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s from %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
            raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

    for fspec, trace_report in zip(files, trace_reports):

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


#@timeout(seconds=10800)
def copy_out(files, **kwargs):
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

    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', os.environ.get('DQ2_LOCAL_SITE_ID', None))
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

        try:
            timeout(ctimeout)(_stage_out_api)(fspec, summary_file_path, trace_report, trace_report_out, transfer_timeout)
            #_stage_out_api(fspec, summary_file_path, trace_report, trace_report_out)
        except Exception as error:
            error_msg = str(error)
            # Try to get a better error message from the traces
            if trace_report_out and trace_report_out[0].get('stateReason'):
                error_msg = trace_report_out[0].get('stateReason')
            logger.info('rucio returned an error: %s' % error_msg)

            error_details = resolve_common_transfer_errors(error_msg, is_stagein=False)
            fspec.status = 'failed'
            fspec.status_code = error_details.get('rcode')
            trace_report.update(clientState=error_details.get('state', 'STAGEOUT_ATTEMPT_FAILED'),
                                stateReason=error_details.get('error'), timeEnd=time())
            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s to %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        if summary:  # resolve final pfn (turl) from the summary JSON
            if not os.path.exists(summary_file_path):
                logger.error('Failed to resolve Rucio summary JSON, wrong path? file=%s' % summary_file_path)
            else:
                with open(summary_file_path, 'rb') as f:
                    summary_json = json.load(f)
                    dat = summary_json.get("%s:%s" % (fspec.scope, fspec.lfn)) or {}
                    fspec.turl = dat.get('pfn')
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
def _stage_in_api(dst, fspec, trace_report, trace_report_out, transfer_timeout):

    # init. download client
    from rucio.client.downloadclient import DownloadClient
    download_client = DownloadClient(logger=logger)

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

    # proceed with the download
    logger.info('_stage_in_api file: %s' % str(f))
    trace_pattern = {}
    if trace_report:
        trace_pattern = trace_report

    # download client raises an exception if any file failed
    if fspec.turl:
        result = download_client.download_pfns([f], 1, trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)
    else:
        result = download_client.download_dids([f], trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)

    logger.debug('Rucio download client returned %s' % result)


def _stage_in_api_bulk(dst, files, trace_reports, trace_report_out):
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

    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', os.environ.get('DQ2_LOCAL_SITE_ID', None))

    # build the list of file dictionaries before calling the download function
    file_list = []
    trace_report = trace_reports.pop()

    for fspec in files:
        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
        trace_report.update(catStart=time())  ## is this metric still needed? LFC catalog
        trace_reports.append(trace_report)

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

        file_list.append(f)

    # proceed with the download
    trace_pattern = {}
    if trace_report:
        trace_pattern = trace_report

    # download client raises an exception if any file failed
    result = download_client.download_pfns(file_list, 1, trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)
    logger.debug('Rucio download client returned %s' % result)


def _stage_out_api(fspec, summary_file_path, trace_report, trace_report_out, transfer_timeout):

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

    # if fspec.storageId and int(fspec.storageId) > 0:
    #     if fspec.turl and fspec.is_nondeterministic:
    #         f['pfn'] = fspec.turl
    # elif fspec.lfn and '.root' in fspec.lfn:
    #     f['guid'] = fspec.guid
    if fspec.lfn and '.root' in fspec.lfn:
        f['guid'] = fspec.guid

    # process with the upload
    logger.info('_stage_out_api: %s' % str(f))
    result = None

    # upload client raises an exception if any file failed
    try:
        result = upload_client.upload([f], summary_file_path=summary_file_path, traces_copy_out=trace_report_out)
    except UnboundLocalError:
        logger.warning('rucio still needs a bug fix of the summary in the uploadclient')

    logger.debug('Rucio upload client returned %s' % result)

    try:
        file_exists = verify_stage_out(fspec)
        logger.info('File exists at the storage: %s' % str(file_exists))
        if not file_exists:
            raise StageOutFailure('stageOut: Physical check after upload failed.')
    except Exception as e:
        msg = 'stageOut: File existence verification failed with: %s' % str(e)
        logger.info(msg)
        raise StageOutFailure(msg)
