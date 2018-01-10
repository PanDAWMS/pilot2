#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import copy
import Queue
import json
import os
import subprocess
import tarfile
import threading
import time
from json import dumps

from pilot.control.job import send_state

import logging
logger = logging.getLogger(__name__)


def control(queues, traces, args):

    threads = [threading.Thread(target=copytool_in,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=copytool_out,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args})]

    [t.start() for t in threads]


def _call(args, executable, cwd=os.getcwd(), logger=logger):
    try:
        process = subprocess.Popen(executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd=cwd)
    except Exception as e:
        logger.error('could not execute: %s' % str(e))
        return False

    logger.info('started -- pid=%s executable=%s' % (process.pid, executable))

    breaker = False
    exit_code = None
    while True:
        for i in xrange(10):
            if args.graceful_stop.is_set():
                breaker = True
                logger.debug('breaking: sending SIGTERM pid=%s' % process.pid)
                process.terminate()
                break
            time.sleep(0.1)
        if breaker:
            logger.debug('breaking: sleep 3s before sending SIGKILL pid=%s' % process.pid)
            time.sleep(3)
            process.kill()
            break

        exit_code = process.poll()
        if exit_code is not None:
            break
        else:
            continue

    logger.info('finished -- pid=%s exit_code=%s' % (process.pid, exit_code))
    stdout, stderr = process.communicate()
    logger.debug('stdout:\n%s' % stdout)
    logger.debug('stderr:\n%s' % stderr)

    if exit_code == 0:
        return True
    else:
        return False


def _stage_in(args, job):
    log = logger.getChild(str(job['PandaID']))

    os.environ['RUCIO_LOGGING_FORMAT'] = '{0}%(asctime)s %(levelname)s [%(message)s]'
    if not _call(args,
                 ['/usr/bin/env',
                  'rucio', '-v', 'download',
                  '--no-subdir',
                  '--rse', job['ddmEndPointIn'],
                  '%s:%s' % (job['scopeIn'], job['inFiles'])],
                 cwd=job['working_dir'],
                 logger=log):
        return False
    return True


def stage_in_auto(site, files):
    """
    Separate dummy implementation for automatic stage-in outside of pilot workflows.
    Should be merged with regular stage-in functionality later, but we need to have
    some operational experience with it first.
    Many things to improve:
     - separate file error handling in the merged case
     - auto-merging of files with same destination into single copytool call
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    executable = ['/usr/bin/env',
                  'rucio', '-v', 'download',
                  '--no-subdir']

    # quickly remove non-existing destinations
    for f in files:
        if not os.path.exists(f['destination']):
            f['status'] = 'failed'
            f['errmsg'] = 'Destination directory does not exist: %s' % f['destination']
            f['errno'] = 1
        else:
            f['status'] = 'running'
            f['errmsg'] = 'File not yet successfully downloaded.'
            f['errno'] = 2

    for f in files:
        if f['errno'] == 1:
            continue

        tmp_executable = copy.deepcopy(executable)

        tmp_executable += ['--dir', f['destination']]
        tmp_executable.append('%s:%s' % (f['scope'],
                                         f['name']))
        process = subprocess.Popen(tmp_executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        f['errno'] = 2
        while True:
            time.sleep(0.1)
            exit_code = process.poll()
            if exit_code is not None:
                stdout, stderr = process.communicate()
                if exit_code == 0:
                    f['status'] = 'done'
                    f['errno'] = 0
                    f['errmsg'] = 'File successfully downloaded.'
                else:
                    f['status'] = 'failed'
                    f['errno'] = 3
                    try:
                        # the Details: string is set in rucio: lib/rucio/common/exception.py in __str__()
                        f['errmsg'] = [detail for detail in stderr.split('\n') if detail.startswith('Details:')][0][9:-1]
                    except Exception as e:
                        f['errmsg'] = 'Could not find rucio error message details - please check stderr directly: %s' % str(e)
                break
            else:
                continue

    return files


def stage_out_auto(site, files):
    """
    Separate dummy implementation for automatic stage-out outside of pilot workflows.
    Should be merged with regular stage-out functionality later, but we need to have
    some operational experience with it first.
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    executable = ['/usr/bin/env',
                  'rucio', '-v', 'upload']

    # quickly remove non-existing destinations
    for f in files:
        if not os.path.exists(f['file']):
            f['status'] = 'failed'
            f['errmsg'] = 'Source file does not exist: %s' % f['file']
            f['errno'] = 1
        else:
            f['status'] = 'running'
            f['errmsg'] = 'File not yet successfully uploaded.'
            f['errno'] = 2

    for f in files:
        if f['errno'] == 1:
            continue

        tmp_executable = copy.deepcopy(executable)

        tmp_executable += ['--rse', f['rse']]

        if 'no_register' in f.keys() and f['no_register']:
            tmp_executable += ['--no-register']

        if 'summary' in f.keys() and f['summary']:
            tmp_executable += ['--summary']

        if 'lifetime' in f.keys():
            tmp_executable += ['--lifetime', str(f['lifetime'])]

        if 'guid' in f.keys():
            tmp_executable += ['--guid', f['guid']]

        if 'attach' in f.keys():
            tmp_executable += ['--scope', f['scope'], '%s:%s' % (f['attach']['scope'], f['attach']['name']), f['file']]
        else:
            tmp_executable += ['--scope', f['scope'], f['file']]

        process = subprocess.Popen(tmp_executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        f['errno'] = 2
        while True:
            time.sleep(0.1)
            exit_code = process.poll()
            if exit_code is not None:
                stdout, stderr = process.communicate()
                if exit_code == 0:
                    f['status'] = 'done'
                    f['errno'] = 0
                    f['errmsg'] = 'File successfully uploaded.'
                else:
                    f['status'] = 'failed'
                    f['errno'] = 3
                    try:
                        # the Details: string is set in rucio: lib/rucio/common/exception.py in __str__()
                        f['errmsg'] = [detail for detail in stderr.split('\n') if detail.startswith('Details:')][0][9:-1]
                    except Exception as e:
                        f['errmsg'] = 'Could not find rucio error message details - please check stderr directly: %s' % str(e)
                break
            else:
                continue

    return files


def copytool_in(queues, traces, args):

    while not args.graceful_stop.is_set():
        try:
            job = queues.data_in.get(block=True, timeout=1)

            send_state(job, args, 'running')

            if _stage_in(args, job):
                queues.finished_data_in.put(job)
            else:
                logger.warning('stage-in failed, adding job object to failed_data_in queue')
                queues.failed_data_in.put(job)

        except Queue.Empty:
            continue


def copytool_out(queues, traces, args):

    while not args.graceful_stop.is_set():
        try:
            job = queues.data_out.get(block=True, timeout=1)

            logger.info('dataset=%s rse=%s' % (job['destinationDblock'], job['ddmEndPointOut'].split(',')[0]))

            # send_state(job, args, 'running')  # not necessary to send job update at this point?

            if _stage_out_all(job, args):
                queues.finished_data_out.put(job)  # needed?
                if job['transExitCode'] == 0:
                    logger.info('Finished stage-out for finished payload')
                    queues.finished_jobs.put(job)
                else:
                    logger.info('Finished stage-out for failed payload')
                    queues.failed_jobs.put(job)
            else:
                queues.failed_data_out.put(job)

        except Queue.Empty:
            continue


def prepare_log(job, tarball_name):
    log = logger.getChild(str(job['PandaID']))
    log.info('preparing log file')

    input_files = job['inFiles'].split(',')
    output_files = job['outFiles'].split(',')
    force_exclude = ['geomDB', 'sqlite200']

    with tarfile.open(name=os.path.join(job['working_dir'], job['logFile']),
                      mode='w:gz',
                      dereference=True) as log_tar:
        for _file in list(set(os.listdir(job['working_dir'])) - set(input_files) - set(output_files) - set(force_exclude)):
            logging.debug('adding to log: %s' % _file)
            log_tar.add(os.path.join(job['working_dir'], _file),
                        arcname=os.path.join(tarball_name, _file))

    return {'scope': job['scopeLog'],
            'name': job['logFile'],
            'guid': job['logGUID'],
            'bytes': os.stat(os.path.join(job['working_dir'], job['logFile'])).st_size}


def _stage_out(args, outfile, job):
    log = logger.getChild(str(job['PandaID']))

    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'
    executable = ['/usr/bin/env',
                  'rucio', '-v', 'upload',
                  '--summary', '--no-register',
                  '--guid', outfile['guid'],
                  '--rse', job['ddmEndPointOut'].split(',')[0],
                  '--scope', outfile['scope'],
                  outfile['name']]

    try:
        process = subprocess.Popen(executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd=job['working_dir'])
    except Exception as e:
        log.error('could not execute: %s' % str(e))
        return None

    log.info('started -- pid=%s executable=%s' % (process.pid, executable))

    breaker = False
    exit_code = None
    while True:
        for i in xrange(10):
            if args.graceful_stop.is_set():
                breaker = True
                log.debug('breaking -- sending SIGTERM pid=%s' % process.pid)
                process.terminate()
                break
            time.sleep(0.1)
        if breaker:
            log.debug('breaking -- sleep 3s before sending SIGKILL pid=%s' % process.pid)
            time.sleep(3)
            process.kill()
            break

        exit_code = process.poll()
        log.info('running -- pid=%s exit_code=%s' % (process.pid, exit_code))
        if exit_code is not None:
            break
        else:
            continue

    log.info('finished -- pid=%s exit_code=%s' % (process.pid, exit_code))
    out, err = process.communicate()
    log.debug('stdout:\n%s' % out)
    log.debug('stderr:\n%s' % err)

    if exit_code is None:
        return None

    summary = None
    with open(os.path.join(job['working_dir'], 'rucio_upload.json'), 'rb') as summary_file:
        summary = json.load(summary_file)

    return summary


def _stage_out_all(job, args):

    log = logger.getChild(str(job['PandaID']))
    outputs = {}

    if 'job_report' in job:
        for f in job['job_report']['files']['output']:
            outputs[f['subFiles'][0]['name']] = {'scope': job['scopeOut'],
                                                 'name': f['subFiles'][0]['name'],
                                                 'guid': f['subFiles'][0]['file_guid'],
                                                 'bytes': f['subFiles'][0]['file_size']}
    else:
        log.warning('Job object does not contain a job report (payload failed?)')
    outputs['%s:%s' % (job['scopeLog'], job['logFile'])] = prepare_log(job, 'tarball_PandaJob_%s_%s' % (job['PandaID'], args.queue))

    infodict = {}
    failed = False

    for outfile in outputs:
        summary = _stage_out(args, outputs[outfile], job)

        if summary is not None:
            outputs[outfile]['pfn'] = summary['%s:%s' % (outputs[outfile]['scope'], outputs[outfile]['name'])]['pfn']
            outputs[outfile]['adler32'] = summary['%s:%s' % (outputs[outfile]['scope'], outputs[outfile]['name'])]['adler32']

            filedict = {'guid': outputs[outfile]['guid'],
                        'fsize': outputs[outfile]['bytes'],
                        'adler32': outputs[outfile]['adler32'],
                        'surl': outputs[outfile]['pfn']}
            infodict[outputs[outfile]['name']] = filedict
        else:
            failed = True

    log.info('infodict=%s' % str(infodict))
    if failed:
        send_state(job, args, 'failed')
        return False
    else:
        send_state(job, args, 'finished', xml=dumps(infodict))
        return True


def _stage_out_all_old(job, args):

    outputs = {}

    for f in job['job_report']['files']['output']:
        outputs[f['subFiles'][0]['name']] = {'scope': job['scopeOut'],
                                             'name': f['subFiles'][0]['name'],
                                             'guid': f['subFiles'][0]['file_guid'],
                                             'bytes': f['subFiles'][0]['file_size']}

    outputs['%s:%s' % (job['scopeLog'], job['logFile'])] = prepare_log(job, 'tarball_PandaJob_%s_%s' % (job['PandaID'], args.queue))

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
 </File>
'''

    failed = False

    for outfile in outputs:
        summary = _stage_out(args, outputs[outfile], job)

        if summary is not None:
            outputs[outfile]['pfn'] = summary['%s:%s' % (outputs[outfile]['scope'], outputs[outfile]['name'])]['pfn']
            outputs[outfile]['adler32'] = summary['%s:%s' % (outputs[outfile]['scope'], outputs[outfile]['name'])]['adler32']

            pfc += pfc_file.format(**outputs[outfile])

        else:
            failed = True

    pfc += '</POOLFILECATALOG>'

    if failed:
        send_state(job, args, 'failed')
        return False
    else:
        send_state(job, args, 'finished', xml=pfc)
        return True
