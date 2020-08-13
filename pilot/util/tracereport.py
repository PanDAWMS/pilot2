# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, alexey.anisenkov@cern.ch, 2017
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import hashlib
import socket
import time
from sys import exc_info
from json import dumps  #, loads
from os import environ, getuid

from pilot.util.config import config
from pilot.util.constants import get_pilot_version, get_rucio_client_version
from pilot.util.container import execute
#from pilot.util.https import request

import logging
logger = logging.getLogger(__name__)


class TraceReport(dict):

    def __init__(self, *args, **kwargs):

        event_version = "%s+%s" % (get_pilot_version(), get_rucio_client_version())
        defs = {
            'eventType': '',                # sitemover
            'eventVersion': event_version,  # Pilot+Rucio client version
            'protocol': None,               # set by specific copy tool
            'clientState': 'INIT_REPORT',
            'localSite': '',                # localsite
            'remoteSite': '',               # equals remotesite
            'timeStart': None,              # time to start
            'catStart': None,
            'relativeStart': None,
            'transferStart': None,
            'validateStart': None,
            'timeEnd': None,
            'dataset': '',
            'version': None,
            'duid': None,
            'filename': None,
            'guid': None,
            'filesize': None,
            'usr': None,
            'appid': None,
            'hostname': '',
            'ip': '',
            'suspicious': '0',
            'usrdn': '',
            'url': None,
            'stateReason': None,
            'uuid': None,
            'taskid': ''
        }

        super(TraceReport, self).__init__(defs)
        self.update(dict(*args, **kwargs))  # apply extra input

    # sitename, dsname, eventType
    def init(self, job):
        """
        Initialization.

        :param job: job object.
        :return:
        """

        data = {
            'clientState': 'INIT_REPORT',
            'usr': hashlib.md5(job.produserid.encode('utf-8')).hexdigest(),  # anonymise user and pilot id's, Python 2/3
            'appid': job.jobid,
            'usrdn': job.produserid,
            'taskid': job.taskid
        }
        self.update(data)
        self['timeStart'] = time.time()

        try:
            self['hostname'] = socket.gethostbyaddr(socket.gethostname())[0]
        except Exception:
            logger.debug("unable to detect hostname for trace report")

        try:
            self['ip'] = socket.gethostbyname(socket.gethostname())
        except Exception:
            logger.debug("unable to detect host IP for trace report")

        if job.jobdefinitionid:
            s = 'ppilot_%s' % job.jobdefinitionid
            self['uuid'] = hashlib.md5(s.encode('utf-8')).hexdigest()  # hash_pilotid, Python 2/3
        else:
            #self['uuid'] = commands.getoutput('uuidgen -t 2> /dev/null').replace('-', '')  # all LFNs of one request have the same uuid
            cmd = 'uuidgen -t 2> /dev/null'
            exit_code, stdout, stderr = execute(cmd)
            self['uuid'] = stdout.replace('-', '')

    def verify_trace(self):
        """
        Verify the trace consistency.
        Are all required fields set? Remove escape chars from stateReason if present.

        :return: Boolean.
        """

        # remove any escape characters that might be present in the stateReason field
        state_reason = self.get('stateReason', '')
        if not state_reason:
            state_reason = ''
        self.update(stateReason=state_reason.replace('\\', ''))

        if not self['eventType'] or not self['localSite'] or not self['remoteSite']:
            return False
        else:
            return True

    def send(self):
        """
        Send trace to rucio server using curl.

        :return: Boolean.
        """

        url = config.Rucio.url
        logger.info("tracing server: %s" % url)
        logger.info("sending tracing report: %s" % str(self))

        if not self.verify_trace():
            logger.warning('cannot send trace since not all fields are set')
            return False

        try:
            # take care of the encoding
            #data = {'API': '0_3_0', 'operation': 'addReport', 'report': self}
            data = dumps(self).replace('"', '\\"')
            #loaded = loads(data)
            #logger.debug('self object converted to json dictionary: %s' % loaded)

            ssl_certificate = self.get_ssl_certificate()

            # create the command
            cmd = 'curl --connect-timeout 20 --max-time 120 --cacert %s -v -k -d \"%s\" %s' % \
                  (ssl_certificate, data, url)
            exit_code, stdout, stderr = execute(cmd)
            if exit_code:
                logger.warning('failed to send traces to rucio: %s' % stdout)
            #request(url, loaded)
            #if status is not None:
            #    logger.warning('failed to send traces to rucio: %s' % status)
            #    raise Exception(status)
        except Exception:
            # if something fails, log it but ignore
            logger.error('tracing failed: %s' % str(exc_info()))
        else:
            logger.info("tracing report sent")

        return True

    def get_ssl_certificate(self):
        """
        Return the path to the SSL certificate

        :return: path (string).
        """

        return environ.get('X509_USER_PROXY', '/tmp/x509up_u%s' % getuid())
