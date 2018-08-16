# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, alexey.anisenkov@cern.ch, 2017
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2018

import time

import hashlib
#import commands
from pilot.util.container import execute

import socket

import pilot.util.https
from sys import exc_info

import logging
logger = logging.getLogger(__name__)


class TraceReport(dict):

    def __init__(self, *args, **kwargs):

        defs = {
            'eventType': '',           # sitemover
            'eventVersion': 'pilot2',  # pilot version
            'protocol': None,          # set by specific sitemover
            'clientState': 'INIT_REPORT',
            'localSite': '',           # localsite
            'remoteSite': '',          # equals remotesite (pilot does not do remote copy?)
            'timeStart': None,         # time to start
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

        data = {
            'clientState': 'INIT_REPORT',
            'usr': hashlib.md5(job.produserid).hexdigest(),  # anonymise user and pilot id's
            'appid': job.jobid,
            'usrdn': job.produserid,
            'taskid': job.taskid
        }
        self.update(data)
        self['timeStart'] = time.time()

        try:
            self['hostname'] = socket.gethostbyaddr(socket.gethostname())[0]
        except Exception:
            logger.debug("Unable to detect hostname for trace report")

        try:
            self['ip'] = socket.gethostbyname(socket.gethostname())
        except Exception:
            logger.debug("Unable to detect host IP for trace report")

        if job.jobdefinitionid:
            self['uuid'] = hashlib.md5('ppilot_%s' % job.jobdefinitionid).hexdigest()  # hash_pilotid
        else:
            #self['uuid'] = commands.getoutput('uuidgen -t 2> /dev/null').replace('-', '')  # all LFNs of one request have the same uuid
            cmd = 'uuidgen -t 2> /dev/null'
            exit_code, stdout, stderr = execute(cmd)
            self['uuid'] = stdout. replace('-', '')

    def send(self):
        url = 'https://rucio-lb-prod.cern.ch/traces/'
        logger.info("Tracing server: %s" % url)
        logger.info("Sending tracing report: %s" % str(self))
        try:
            # take care of the encoding
            data = {'API': '0_3_0', 'operation': 'addReport', 'report': self}
            #from json import dumps
            #data = dumps(self).replace('"', '\\"')

            #sslCertificate = si.getSSLCertificate()

            # create the command
            #cmd = 'curl --connect-timeout 20 --max-time 120 --cacert %s -v -k -d "%s" %s' % (sslCertificate, data, url)
            status = pilot.util.https.request(url, data)
            #logger.info("Executing command: %s" % (cmd))
            #s, o = commands.getstatusoutput(cmd)
            if status is not None:
                raise Exception(status)
        except Exception:
            # if something fails, log it but ignore
            logger.error('tracing failed: %s' % str(exc_info()))
        else:
            logger.info("Tracing report sent")
