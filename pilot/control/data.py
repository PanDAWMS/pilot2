#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017

import Queue
import commands
import json
import os
import subprocess
import tarfile
import threading
import time
import urllib

from pilot.util import information

import logging
logger = logging.getLogger(__name__)


def control(queues, graceful_stop, traces, args):

    threads = [threading.Thread(target=copytool_in,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=copytool_out,
                                kwargs={'queues': queues,
                                        'graceful_stop': graceful_stop,
                                        'traces': traces,
                                        'args': args})]

    storages = information.get_storages()

    if args.site not in [s['site'] for s in storages]:
        logger.warning('no configured storage found for site {0}'.format(args.site))

    storage = []
    for s in storages:
        if args.site == s['site']:
            storage.append(s)
            break

    [t.start() for t in threads]


def __stage_in(job, graceful_stop):

    executable = ['rucio', '-v', 'download',
                  '--no-subdir',
                  '--rse', job['ddmEndPointIn'],
                  job['realDatasetsIn']]

    try:
        process = subprocess.Popen(executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd='job-{0}'.format(job['PandaID']))
    except Exception as e:
        logger.error('could not execute: {0}'.format(e))
        return False

    logger.info('{0}: started -- pid={1} executable={2}'.format(job['PandaID'], process.pid, executable))

    breaker = False
    while True:
        for i in xrange(10):
            if graceful_stop.is_set():
                breaker = True
                logger.debug('{0}: breaking: sending SIGTERM pid={1}'.format(job['PandaID'], process.pid))
                process.terminate()
                break
            time.sleep(0.1)
        if breaker:
            logger.debug('{0}: breaking: sleep 3s before sending SIGKILL pid={1}'.format(job['PandaID'], process.pid))
            time.sleep(3)
            process.kill()
            break

        exit_code = process.poll()
        logger.info('{0}: running -- pid={1} exit_code={2}'.format(job['PandaID'], process.pid, exit_code))
        if exit_code is not None:
            break
        else:
            continue

    logger.info('{0}: finished -- pid={0} exit_code={1}'.format(job['PandaID'], process.pid, exit_code))
    stdout, stderr = process.communicate()
    logger.debug('{0}: stdout:\n{1}'.format(job['PandaID'], stdout))
    logger.debug('{0}: stderr:\n{1}'.format(job['PandaID'], stderr))

    if exit_code == 0:
        return True
    else:
        return False


def copytool_in(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.data_in.get(block=True, timeout=1)
            logger.info('{0}: dataset={1} rse={2}'.format(job['PandaID'], job['realDatasetsIn'], job['ddmEndPointIn']))

            logger.debug('{0}: set job state=transferring'.format(job['PandaID']))
            cmd = 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates --cert ' \
                  '$X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY "https://pandaserver.cern.ch:25443/server/panda/updateJob?' \
                  'jobId={0}&state=transferring"'.format(job['PandaID'])
            logger.debug('executing: {0}'.format(cmd))
            s, o = commands.getstatusoutput(cmd)
            if s != 0:
                logger.warning('{0}: set job state=transferring failed: {1}'.format(job['PandaID'], o))
            else:
                logger.info('{0}: confirmed job state=transferring'.format(job['PandaID']))

            if __stage_in(job, graceful_stop):
                queues.finished_data_in.put(job)
            else:
                queues.failed_data_in.put(job)

        except Queue.Empty:
            continue


def copytool_out(queues, graceful_stop, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.data_out.get(block=True, timeout=1)
            logger.info('{0}: dataset={1} rse={2}'.format(job['PandaID'], job['destinationDblock'], job['ddmEndPointOut']))

            logger.debug('{0}: set job state=transferring'.format(job['PandaID']))
            cmd = 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates --cert' \
                  ' $X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY "https://pandaserver.cern.ch:25443/server/panda/updateJob?jobId={0}' \
                  '&state=transferring"'.format(job['PandaID'])
            logger.debug('executing: {0}'.format(cmd))
            s, o = commands.getstatusoutput(cmd)
            if s != 0:
                logger.warning('{0}: set job state=transferring failed: {1}'.format(job['PandaID'], o))
            else:
                logger.info('{0}: confirmed job state=transferring'.format(job['PandaID']))

            if __stage_out(job, args, graceful_stop):
                queues.finished_data_out.put(job)
            else:
                queues.failed_data_out.put(job)

        except Queue.Empty:
            continue


def __stage_out(job, args, graceful_stop):

    __files = []

    infiles = job['inFiles'].split(',')
    outfiles = job['outFiles'].split(',')
    ddm_end_point_out = job['ddmEndPointOut'].split(',')

    for i in xrange(len(outfiles)):
        if outfiles[i] == job['logFile']:

            with tarfile.open(name='job-{0}/{1}'.format(job['PandaID'], outfiles[i]),
                              mode='w:gz',
                              dereference=True) as log_tar:
                for log_file in list(set(os.listdir('job-{0}'.format(job['PandaID']))) - set(infiles) - set(outfiles)):
                    os.system('/usr/bin/sync')
                    log_tar.add('job-{0}/{1}'.format(job['PandaID'], log_file),
                                arcname='tarball_PandaJob_{0}_{1}/{2}'.format(job['PandaID'],
                                                                              args.queue,
                                                                              log_file))

            __files.append({'scope': job['scopeLog'],
                            'name': outfiles[i],
                            'guid': job['logGUID'],
                            'ddm': ddm_end_point_out[i],
                            'bytes': os.stat('job-{0}/{1}'.format(job['PandaID'], outfiles[i])).st_size,
                            'adler32': None})
        else:
            __files.append({'scope': job['scopeOut'],
                            'name': outfiles[i],
                            'guid': None,
                            'ddm': ddm_end_point_out[i],
                            'bytes': None,
                            'adler32': None})

            for f in job['jobReport']['files']['output']:
                if f['subFiles'][0]['name'] == outfiles[i]:
                    __files[-1]['guid'] = f['subFiles'][0]['file_guid']
                    __files[-1]['bytes'] = f['subFiles'][0]['file_size']

    for file in __files:

        executable = ['rucio', '-v', 'upload',
                      '--summary', '--no-register',
                      '--guid', file['guid'],
                      '--rse', file['ddm'],
                      '--scope', file['scope'],
                      file['name']]

        try:
            process = subprocess.Popen(executable,
                                       bufsize=-1,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       cwd='job-{0}'.format(job['PandaID']))
        except Exception as e:
            logger.error('could not execute: {0}'.format(e))
            return False

        logger.info('{0}: started -- pid={1} executable={2}'.format(job['PandaID'], process.pid, executable))

        breaker = False
        while True:
            for i in xrange(10):
                if graceful_stop.is_set():
                    breaker = True
                    logger.debug('{0}: breaking -- sending SIGTERM pid={1}'.format(job['PandaID'], process.pid))
                    process.terminate()
                    break
                time.sleep(0.1)
            if breaker:
                logger.debug('{0}: breaking -- sleep 3s before sending SIGKILL pid={1}'.format(job['PandaID'], process.pid))
                time.sleep(3)
                process.kill()
                break

            exit_code = process.poll()
            logger.info('{0}: running -- pid={1} exit_code={2}'.format(job['PandaID'], process.pid, exit_code))
            if exit_code is not None:
                break
            else:
                continue

        logger.info('{0}: finished -- pid={1} exit_code={2}'.format(job['PandaID'], process.pid, exit_code))
        stdout, stderr = process.communicate()
        logger.debug('{0}: stdout:\n{1}'.format(job['PandaID'], stdout))
        logger.debug('{0}: stderr:\n{1}'.format(job['PandaID'], stderr))

        summary = None
        with open('job-{0}/{1}'.format(job['PandaID'], 'rucio_upload.json'), 'rb') as summary_file:
            summary = json.load(summary_file)

        file['pfn'] = summary['{0}:{1}'.format(file['scope'], file['name'])]['pfn']
        file['adler32'] = summary['{0}:{1}'.format(file['scope'], file['name'])]['adler32']

    pfc = '''<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>'''

    pfc_file = '''
 <File ID="{0}">
  <logical>
   <lfn name="{1}"/>
  </logical>
  <metadata att_name="surl" att_value="{2}"/>
  <metadata att_name="fsize" att_value="{3}"/>
  <metadata att_name="adler32" att_value="{4}"/>
 </File>'''

    for file in __files:
        pfc += pfc_file.format(file['guid'],
                               file['name'],
                               file['pfn'],
                               file['bytes'],
                               file['adler32'])

    pfc += '</POOLFILECATALOG>'

    if exit_code == 0:

        logger.debug('{0}: set job state=finished'.format(job['PandaID']))
        print 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates --cert' \
              ' $X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY "https://pandaserver.cern.ch:25443/server/panda/updateJob?jobId=' \
              '{0}&state=finished&xml={1}"'.format(job['PandaID'], urllib.quote_plus(pfc))
        cmd = 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates --cert' \
              ' $X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY "https://pandaserver.cern.ch:25443/server/panda/updateJob?jobId=' \
              '{0}&state=finished&xml={1}"'.format(job['PandaID'], urllib.quote_plus(pfc))
        logger.debug('executing: {0}'.format(cmd))
        s, o = commands.getstatusoutput(cmd)
        if s != 0:
            logger.warning('{0}: set job state=finished failed: {1}'.format(job['PandaID'], o))
        else:
            logger.info('{0}: confirmed job state=finished'.format(job['PandaID']))

        return True
    else:

        logger.debug('{0}: set job state=failed'.format(job['PandaID']))
        print 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates --cert' \
              ' $X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY "https://pandaserver.cern.ch:25443/server/panda/updateJob?jobId=' \
              '{0}&state=failed"'.format(job['PandaID'])
        cmd = 'curl -sS -H "Accept: application/json" --connect-timeout 1 --max-time 3 --compressed --capath /etc/grid-security/certificates --cert' \
              ' $X509_USER_PROXY --cacert $X509_USER_PROXY --key $X509_USER_PROXY "https://pandaserver.cern.ch:25443/server/panda/updateJob?jobId=' \
              '{0}&state=failed"'.format(job['PandaID'])
        logger.debug('executing: {0}'.format(cmd))
        s, o = commands.getstatusoutput(cmd)
        if s != 0:
            logger.warning('{0}: set job state=failed failed: {1}'.format(job['PandaID'], o))
        else:
            logger.info('{0}: confirmed job state=failed'.format(job['PandaID']))

        return False
