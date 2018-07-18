# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018

import logging
import os
import shutil
import sys
import traceback
import uuid

from pilot.api.data import StageOutClient
from pilot.common import exception
from pilot.info.filespec import FileSpec

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def check_env():
    """
    Function to check whether cvmfs is available.
    To be used to decide whether to skip some test functions.

    :returns True: if cvmfs is available. Otherwise False.
    """
    return os.path.exists('/cvmfs/atlas.cern.ch/repo/')


@unittest.skipIf(not check_env(), "No CVMFS")
class TestStageOut(unittest.TestCase):
    """
    Unit tests for event service Grid work executor
    """

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_stageout_pw(self):
        """
        Make sure that no exceptions to stage out file.
        """
        error = None
        try:
            from pilot.info import infosys, InfoService
            infoservice = InfoService()
            infoservice.init('BNL_CLOUD_MCORE', infosys.confinfo, infosys.extinfo)

            output_file = os.path.join('/tmp', str(uuid.uuid4()))
            shutil.copy('/bin/hostname', output_file)
            file_data = {'scope': 'transient',
                         'lfn': os.path.basename(output_file),
                         #'ddmendpoint': None,
                         #'type': 'es_events',
                         #'surl': output_file
                         #'turl': None,
                         #'filesize': None,
                         #'checksum': None
                         }
            file_spec = FileSpec(type='output', **file_data)
            xdata = [file_spec]
            workdir = os.path.dirname(output_file)
            client = StageOutClient(infoservice)
            kwargs = dict(workdir=workdir, cwd=workdir, usecontainer=False)
            client.transfer(xdata, activity=['pw'], **kwargs)
        except exception.PilotException, error:
            logger.error("Pilot Exeception: %s, %s" % (error.get_detail(), traceback.format_exc()))
        except Exception, e:
            logger.error(traceback.format_exc())
            error = exception.StageOutFailure("stageOut failed with error=%s" % e)

        logger.info('Summary of transferred files:')
        for e in xdata:
            logger.info(" -- lfn=%s, status_code=%s, status=%s" % (e.lfn, e.status_code, e.status))

        if error:
            logger.error('Failed to stage-out eventservice file(%s): error=%s' % (output_file, error.get_detail()))
            raise error

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_stageout_es_events(self):
        """
        Make sure that no exceptions to stage out file.
        """
        error = None
        try:
            from pilot.info import infosys, InfoService
            infoservice = InfoService()
            infoservice.init('BNL_CLOUD_MCORE', infosys.confinfo, infosys.extinfo)

            output_file = os.path.join('/tmp', str(uuid.uuid4()))
            shutil.copy('/bin/hostname', output_file)
            file_data = {'scope': 'transient',
                         'lfn': os.path.basename(output_file),
                         #'ddmendpoint': None,
                         #'type': 'es_events',
                         #'surl': output_file
                         #'turl': None,
                         #'filesize': None,
                         #'checksum': None
                         }
            file_spec = FileSpec(type='output', **file_data)
            xdata = [file_spec]
            workdir = os.path.dirname(output_file)
            client = StageOutClient(infoservice)
            kwargs = dict(workdir=workdir, cwd=workdir, usecontainer=False)
            client.transfer(xdata, activity=['es_events', 'pw'], **kwargs)
        except exception.PilotException, error:
            logger.error("Pilot Exeception: %s, %s" % (error.get_detail(), traceback.format_exc()))
        except Exception, e:
            logger.error(traceback.format_exc())
            error = exception.StageOutFailure("stageOut failed with error=%s" % e)

        logger.info('Summary of transferred files:')
        for e in xdata:
            logger.info(" -- lfn=%s, status_code=%s, status=%s" % (e.lfn, e.status_code, e.status))

        if error:
            logger.error('Failed to stage-out eventservice file(%s): error=%s' % (output_file, error.get_detail()))
            raise error
