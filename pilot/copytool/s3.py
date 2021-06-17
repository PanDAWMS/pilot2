#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

import os
import logging

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:
    pass

from .common import resolve_common_transfer_errors
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException
from pilot.util.ruciopath import get_rucio_path

logger = logging.getLogger(__name__)
errors = ErrorCodes()

require_replicas = False    ## indicates if given copytool requires input replicas to be resolved
require_input_protocols = True    ## indicates if given copytool requires input protocols and manual generation of input replicas
require_protocols = True  ## indicates if given copytool requires protocols to be resolved first for stage-out

allowed_schemas = ['srm', 'gsiftp', 'https', 'davs', 'root', 's3', 's3+rucio']


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files):
    return True  ## FIX ME LATER


def resolve_surl(fspec, protocol, ddmconf, **kwargs):
    """
        Get final destination SURL for file to be transferred to Objectstore
        Can be customized at the level of specific copytool

        :param protocol: suggested protocol
        :param ddmconf: full ddm storage data
        :param fspec: file spec data
        :return: dictionary {'surl': surl}
    """
    ddm = ddmconf.get(fspec.ddmendpoint)
    if not ddm:
        raise PilotException('failed to resolve ddmendpoint by name=%s' % fspec.ddmendpoint)

    if ddm.is_deterministic:
        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), get_rucio_path(fspec.scope, fspec.lfn))
    elif ddm.type in ['OS_ES', 'OS_LOGS']:
        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), fspec.lfn)
        fspec.protocol_id = protocol.get('id')
    else:
        raise PilotException('resolve_surl(): Failed to construct SURL for non deterministic ddm=%s: NOT IMPLEMENTED', fspec.ddmendpoint)

    # example:
    #   protocol = {u'path': u'/atlas-eventservice', u'endpoint': u's3://s3.cern.ch:443/', u'flavour': u'AWS-S3-SSL', u'id': 175}
    #   surl = 's3://s3.cern.ch:443//atlas-eventservice/EventService_premerge_24706191-5013009653-24039149400-322-5.tar'
    return {'surl': surl}


def copy_in(files, **kwargs):
    """
    Download given files from an S3 bucket.

    :param files: list of `FileSpec` objects
    :raise: PilotException in case of controlled error
    """

    for fspec in files:

        dst = fspec.workdir or kwargs.get('workdir') or '.'

        bucket = 'bucket'  # UPDATE ME
        path = os.path.join(dst, fspec.lfn)
        logger.info('downloading object %s from bucket=%s to local file %s', fspec.lfn, bucket, path)
        status, diagnostics = download_file(path, bucket, object_name=fspec.lfn)

        if not status:  ## an error occurred
            error = resolve_common_transfer_errors(diagnostics, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def download_file(path, bucket, object_name=None):
    """
    Download a file from an S3 bucket.

    :param path: Path to local file after download (string).
    :param bucket: Bucket to download from.
    :param object_name: S3 object name. If not specified then file_name from path is used.
    :return: True if file was uploaded (else False), diagnostics (string).
    """

    # if S3 object_name was not specified, use file name from path
    if object_name is None:
        object_name = os.path.basename(path)

    try:
        s3 = boto3.client('s3')
        s3.download_file(bucket, object_name, path)
    except ClientError as error:
        diagnostics = 'S3 ClientError: %s' % error
        logger.critical(diagnostics)
        return False, diagnostics
    except Exception as error:
        diagnostics = 'exception caught in s3_client: %s' % error
        logger.critical(diagnostics)
        return False, diagnostics

    return True, ""


def copy_out(files, **kwargs):
    """
    Upload given files to S3 storage.

    :param files: list of `FileSpec` objects
    :raise: PilotException in case of controlled error
    """

    workdir = kwargs.pop('workdir')

    for fspec in files:

        path = os.path.join(workdir, fspec.lfn)
        if os.path.exists(path):
            bucket = 'bucket'  # UPDATE ME
            logger.info('uploading %s to bucket=%s using object name=%s', path, bucket, fspec.lfn)
            status, diagnostics = upload_file(path, bucket, object_name=fspec.lfn)

            if not status:  ## an error occurred
                # create new error code(s) in ErrorCodes.py and set it/them in resolve_common_transfer_errors()
                error = resolve_common_transfer_errors(diagnostics, is_stagein=False)
                fspec.status = 'failed'
                fspec.status_code = error.get('rcode')
                raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))
        else:
            diagnostics = 'local output file does not exist: %s' % path
            logger.warning(diagnostics)
            fspec.status = 'failed'
            fspec.status_code = errors.STAGEOUTFAILED
            raise PilotException(diagnostics, code=fspec.status_code, state=fspec.status)

        fspec.status = 'transferred'
        fspec.status_code = 0

    return files


def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket.

    :param file_name: File to upload.
    :param bucket: Bucket to upload to.
    :param object_name: S3 object name. If not specified then file_name is used.
    :return: True if file was uploaded (else False), diagnostics (string).
    """

    # if S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # upload the file
    try:
        s3_client = boto3.client('s3')
        #response = s3_client.upload_file(file_name, bucket, object_name)
        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as error:
        diagnostics = 'S3 ClientError: %s' % error
        logger.critical(diagnostics)
        return False, diagnostics
    except Exception as error:
        diagnostics = 'exception caught in s3_client: %s' % error
        logger.critical(diagnostics)
        return False, diagnostics

    return True, ""
