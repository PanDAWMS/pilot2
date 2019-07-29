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

from __future__ import absolute_import

import os
import json
import logging
from time import time

from .common import resolve_common_transfer_errors, verify_catalog_checksum, get_timeout
from pilot.common.exception import PilotException, ErrorCodes

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


# @timeout(seconds=600)
def copy_in(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :param ignore_errors: boolean, if specified then transfer failures will be ignored
        :raise: PilotException in case of controlled error
    """

    allow_direct_access = kwargs.get('allow_direct_access')
    ignore_errors = kwargs.get('ignore_errors')
    trace_report = kwargs.get('trace_report')

    # reference to a list where rucio trace candidates are appended
    traces_rucio = []

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    localsite = os.environ.get('DQ2_LOCAL_SITE_ID', None)
    for fspec in files:
        logger.info('rucio copytool, downloading file with scope:%s lfn:%s' % (str(fspec.scope), str(fspec.lfn)))

        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
        trace_report.update(copytool='rucio')  # hack, should be in api.data

        # continue loop for files that are to be accessed directly
        if fspec.is_directaccess(ensure_replica=False) and allow_direct_access and fspec.accessmode == 'direct':
            fspec.status_code = 0
            fspec.status = 'remote_io'
            trace_report.update(url=fspec.turl, clientState='FOUND_ROOT', stateReason='direct_access')
            trace_report.send()
            continue

        trace_report.update(catStart=time())  ## is this metric still needed? LFC catalog
        fspec.status_code = 0
        dst = fspec.workdir or kwargs.get('workdir') or '.'
        logger.info('the file will be stored in %s' % str(dst))

        # process the stage in
        error_msg = None
        rucio_state = None
        try:
            rucio_state = _stage_in_api(dst, fspec, trace_report, traces_rucio)
        except Exception as error:
            error_msg = str(error)

        # treatment of error states
        if error_msg:
            logger.info('stderr = %s' % error_msg)
        else:
            logger.info('rucio client state: %s' % str(rucio_state))

        if error_msg:
            error = resolve_common_transfer_errors(error_msg, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            trace_report.update(clientState=rucio_state or 'STAGEIN_ATTEMPT_FAILED',
                                stateReason=error_msg, timeEnd=time())
            if not ignore_errors:
                if traces_rucio:
                    _merge_traces([trace_report], traces_rucio)
                trace_report.send()
                raise PilotException(error_msg, code=error.get('rcode'), state='FAILED')

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        # terminating with sending the trace
        destination = os.path.join(dst, fspec.lfn)
        if os.path.exists(destination):
            state, diagnostics = verify_catalog_checksum(fspec, destination)
            if diagnostics != "" and not ignore_errors:
                trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                    timeEnd=time())
                if traces_rucio:
                    _merge_traces([trace_report], traces_rucio)
                trace_report.send()
                raise PilotException(diagnostics, code=fspec.status_code, state=state)
        else:
            logger.warning('file does not exist: %s (cannot verify catalog checksum)' % destination)

        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())

        if traces_rucio:
            _merge_traces([trace_report], traces_rucio)

        trace_report.send()

    return files


# @timeout(seconds=600)
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

    for fspec in files:
        logger.info('rucio copytool, uploading file with scope: %s and lfn: %s' % (str(fspec.scope), str(fspec.lfn)))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset, url=fspec.surl, filesize=fspec.filesize)
        trace_report.update(catStart=time(), filename=fspec.lfn, guid=fspec.guid.replace('-', ''))

        summary_file_path = None
        cwd = fspec.workdir or kwargs.get('workdir') or '.'
        if summary:
            summary_file_path = os.path.join(cwd, 'rucio_upload.json')

        logger.info('the file will be uploaded to %s' % str(fspec.ddmendpoint))
        rucio_state = None
        error_msg = None
        try:
            rucio_state = _stage_out_api(fspec, summary_file_path, trace_report)
        except Exception as error:
            error_msg = str(error)

        if error_msg:
            logger.info('stderr = %s' % error_msg)
        else:
            logger.info('rucio client state: %s' % str(rucio_state))

        fspec.status_code = 0

        if error_msg:  ## error occurred
            error = resolve_common_transfer_errors(error_msg, is_stagein=False)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            trace_report.update(clientState=error.get('state', None) or 'STAGEOUT_ATTEMPT_FAILED',
                                stateReason=error.get('error', 'unknown error'),
                                timeEnd=time())
            if not ignore_errors:
                trace_report.send()
                raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

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
                                           str(local_checksum), str(adler32))
        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())

        trace_report.send()

    return files


def _stage_in_api(dst, fspec, trace_report, traces_rucio):
    """
    Calling the rucio downloadclient.

    :param dst: destination path for the download.
    :param fspec: FileSpec object. (Only one file for the moment.)
    :param trace_report: trace introduced in pilot.
    :param traces_rucio: reference to a list to which the rucio traces are attachd.

    :return: client_state
    """

    # init. download client
    from rucio.client.downloadclient import DownloadClient
    download_client = DownloadClient(logger=logger)

    # traces are switched off
    if hasattr(download_client, 'tracing'):
        download_client.tracing = tracing_rucio

    # file specifications before the actual download
    f = {'did_scope': fspec.scope, 'did_name': fspec.lfn, 'rse': fspec.ddmendpoint, 'base_dir': dst, 'no_subdir': True}
    f['did'] = '%s:%s' % (fspec.scope, fspec.lfn)
    if fspec.turl:
        f['pfn'] = fspec.turl

    # getting transfer timeout
    if fspec.filesize:
        f['transfer_timeout'] = get_timeout(fspec.filesize)

    # process the download
    logger.info('_stage_in_api file: %s' % str(f))
    trace_pattern = {}
    if trace_report:
        trace_pattern = trace_report
    result = []
    if fspec.turl:
        result = download_client.download_pfns([f], 1, trace_custom_fields=trace_pattern, traces_copy_out=traces_rucio)
    else:
        result = download_client.download_dids([f], trace_custom_fields=trace_pattern, traces_copy_out=traces_rucio)

    client_state = 'FAILED'
    if result:
        client_state = result[0].get('clientState', 'FAILED')

    return client_state


def _stage_out_api(fspec, summary_file_path, trace_report):
    """
    Calling the rucio uploadclient.

    :param fspec: FileSpec object. (Only one file for the moment.)
    :param summary_file_path: summary of the download.
    :param trace_report: trace introduced in pilot.

    :return: client_state
    """

    # init. download client
    from rucio.client.uploadclient import UploadClient
    upload_client = UploadClient(logger=logger)

    # traces are turned off
    if hasattr(upload_client, 'tracing'):
        upload_client.tracing = tracing_rucio
    if tracing_rucio:
        upload_client.trace = trace_report

    # file specifications before the upload
    f = {'rse': fspec.ddmendpoint, 'did_scope': fspec.scope, 'no_register': True}
    f['path'] = fspec.surl or getattr(fspec, 'pfn', None) or os.path.join(fspec.workdir, fspec.lfn)

    # getting transfer timeout
    if fspec.filesize:
        f['transfer_timeout'] = get_timeout(fspec.filesize)

    # guid needs to be set for root files
    if fspec.lfn and '.root' in fspec.lfn:
        f['guid'] = fspec.guid

    # process the upload
    logger.info('_stage_out_api: %s' % str(f))
    result = None
    try:
        result = upload_client.upload([f], summary_file_path)
    except UnboundLocalError:
        logger.warning('rucio still needs a bug fix of the summary in the uploadclient')
        result = 0

    client_state = 'FAILED'
    if result == 0:
        client_state = 'DONE'

    return client_state


def _merge_traces(pilot_traces, rucio_traces):
    """
    This method adds information filled in traces on Rucio side
    with the traces that have been created by pilot.

    :param pilot_traces: list of traces created by pilot
    :param rucio_traces: list of traces created by downloadclient in Rucio
    """

    for p_trace in pilot_traces:
        try:
            r_traces = list(filter(lambda rt: rt['filename'] == p_trace['filename'] and rt['scope'] == p_trace['scope'], rucio_traces))
            if r_traces:
                p_trace.update(r_traces[-1]):
                logger.info('The trace updated for rucio info.: %s' % p_trace)
            else:
                logger.warning('Pilot and rucio traces do not match.')
        except KeyError:
            logger.warning('The pilot trace is missing scope or filename.')
        except Exception as error:
            logger.warning('Traces from pilot and rucio could not be merged: %s' % str(error))
