#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

import Queue
import json
import os
import subprocess
import tarfile
import threading
import time

from pilot.util import information
from pilot.control.job import send_state
from pilot.util import signalling

import logging
logger = logging.getLogger(__name__)

graceful_stop = signalling.graceful_stop_event()


def control(queues, traces, args):

    threads = [threading.Thread(target=copytool_in,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=copytool_out,
                                kwargs={'queues': queues,
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


def _call(executable, cwd=os.getcwd(), logger=logger, graceful_stop=None):
    try:
        process = subprocess.Popen(executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd=cwd)
    except Exception as e:
        logger.error('could not execute: {0}'.format(e))
        return False

    logger.info('started -- pid={0} executable={1}'.format(process.pid, executable))

    breaker = False
    exit_code = None
    while True:
        for i in xrange(10):
            if graceful_stop.is_set():
                breaker = True
                logger.debug('breaking: sending SIGTERM pid={0}'.format(process.pid))
                process.terminate()
                break
            time.sleep(0.1)
        if breaker:
            logger.debug('breaking: sleep 3s before sending SIGKILL pid={0}'.format(process.pid))
            time.sleep(3)
            process.kill()
            break

        exit_code = process.poll()
        logger.info('running -- pid={0} exit_code={1}'.format(process.pid, exit_code))
        if exit_code is not None:
            break
        else:
            continue

    logger.info('finished -- pid={0} exit_code={1}'.format(process.pid, exit_code))
    stdout, stderr = process.communicate()
    logger.debug('stdout:\n{0}'.format(stdout))
    logger.debug('stderr:\n{0}'.format(stderr))

    if exit_code == 0:
        return True
    else:
        return False


def _stage_in(job):
    log = logger.getChild(job['job_id'])

    for f in job['input_files']:
        if not _call(['rucio', '-v', 'download',
                      '--no-subdir',
                      '--rse', job['input_files'][f]['ddm_endpoint'],
                      job['input_files'][f]['scope'] + ":" + f],
                     cwd=job['working_dir'],
                     logger=log):
            return False
    return True


def copytool_in(queues, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.data_in.get(block=True, timeout=1)

            send_state(job, 'transferring')

            if _stage_in(job):
                queues.finished_data_in.put(job)
            else:
                queues.failed_data_in.put(job)

        except Queue.Empty:
            continue


def copytool_out(queues, traces, args):

    while not graceful_stop.is_set():
        try:
            job = queues.data_out.get(block=True, timeout=1)

            # logger.info('dataset={0} rse={1}'.format(job['destinationDblock'], job['ddmEndPointOut']))

            send_state(job, 'transferring')

            if _stage_out(job, args):
                queues.finished_data_out.put(job)
            else:
                queues.failed_data_out.put(job)

        except Queue.Empty:
            continue


def prepare_log(job, tarball_name):
    log = logger.getChild(job['job_id'])
    log.info('Saving log file')

    input_files = job['input_files'].keys()
    output_files = job['output_files'].keys()

    with tarfile.open(name=os.path.join(job['working_dir'], job['log_file']),
                      mode='w:gz',
                      dereference=True) as log_tar:
        for _file in list(set(os.listdir(job['working_dir'])) - set(input_files) - set(output_files)):
            os.system('/usr/bin/sync')
            log_tar.add(os.path.join(job['working_dir'], _file),
                        arcname=os.path.join(tarball_name, _file))

    job['output_files'][job['log_file']]['bytes'] = os.stat(os.path.join(job['working_dir'], job['log_file'])).st_size


def save_file(filename, _file, job):
    log = logger.getChild(job['job_id'])

    executable = ['rucio', '-v', 'upload',
                  '--summary', '--no-register',
                  '--guid', _file['guid'],
                  '--rse', _file['ddm_endpoint'],
                  '--scope', _file['scope'],
                  filename]

    try:
        process = subprocess.Popen(executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd=job['working_dir'])
    except Exception as e:
        log.error('could not execute: {0}'.format(e))
        return None

    log.info('started -- pid={0} executable={1}'.format(process.pid, executable))

    breaker = False
    exit_code = None
    while True:
        for i in xrange(10):
            if graceful_stop.is_set():
                breaker = True
                log.debug('breaking -- sending SIGTERM pid={0}'.format(process.pid))
                process.terminate()
                break
            time.sleep(0.1)
        if breaker:
            log.debug('breaking -- sleep 3s before sending SIGKILL pid={0}'.format(process.pid))
            time.sleep(3)
            process.kill()
            break

        exit_code = process.poll()
        log.info('running -- pid={0} exit_code={1}'.format(process.pid, exit_code))
        if exit_code is not None:
            break
        else:
            continue

    log.info('finished -- pid={0} exit_code={1}'.format(process.pid, exit_code))
    out, err = process.communicate()
    log.debug('stdout:\n{0}'.format(out))
    log.debug('stderr:\n{0}'.format(err))

    if exit_code is None:
        return None

    summary = None
    with open(os.path.join(job['working_dir'], 'rucio_upload.json'), 'rb') as summary_file:
        summary = json.load(summary_file)

    return summary


def _stage_out(job, args):
    for f in job['job_report']['files']['output']:
        fn = f['subFiles'][0]['name']
        job['output_files'][fn]['guid'] = f['subFiles'][0]['file_guid']
        job['output_files'][fn]['bytes'] = f['subFiles'][0]['file_size']

    prepare_log(job, 'tarball_PandaJob_{0}_{1}'.format(job['job_id'], args.queue))

    pfc = '''<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>'''

    pfc_file = '''
 <File ID="{guid}">
  <logical>
   <lfn name="{name}"/>
  </logical>
  <metadata att_name="surl" att_value="{pfn}"/>
  <metadata att_name="fsize" att_value="{bytes}"/>
  <metadata att_name="adler32" att_value="{adler32}"/>
 </File>'''

    failed = False

    for fn, f in job['output_files']:
        summary = save_file(fn, f, job)

        if summary is not None:
            f['pfn'] = summary['{0}:{1}'.format(f['scope'], fn)]['pfn']
            f['adler32'] = summary['{0}:{1}'.format(f['scope'], fn)]['adler32']

            pfc += pfc_file.format(**f)

        else:
            failed = True

    pfc += '</POOLFILECATALOG>'

    if not failed:

        send_state(job, 'finished', xml=pfc)

        return True
    else:

        send_state(job, 'failed')

        return False
