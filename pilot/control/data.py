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
# - Wen Guan, wen.guan@cern.ch, 2018
# - Tobias Wegner, tobias.wegner@cern.ch, 2018

import copy
import Queue
import json
import os
import subprocess
import tarfile
import threading
import time

from pilot.api.data import StageInClient, StageOutClient
from pilot.control.job import send_state
from pilot.common.errorcodes import ErrorCodes

import logging

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def control(queues, traces, args):

    threads = [threading.Thread(target=copytool_in,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=copytool_out,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=queue_monitoring,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args})]

    [t.start() for t in threads]

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

            copytools = job.infosys.queuedata.copytools
            logger.info('Test job.infosys: queuedata.copytools=%s' % copytools)
            output = None
            try:
                stageInClient = StageInClient(copytool_names=copytools, logger=logger)
                files={'destination': job['ddmEndPointIn'],
                        'scope': job['scopeIn'],
                        'name': job['inFiles']}
                output = stageInClient.transfer(files)
            except Exception as error:
                logger.debug('Error: %s' % error)

            if not output or output['status'] == 'failed':
                logger.warning('stage-in failed, adding job object to failed_data_in queue')
                queues.failed_data_in.put(job)
                job['pilotErrorCodes'], job['pilotErrorDiags'] = errors.add_error_code(errors.STAGEINFAILED)
                job['state'] = "failed"
                # send_state(job, args, 'failed')

        except Queue.Empty:
            continue


def copytool_out(queues, traces, args):

    while not args.graceful_stop.is_set():
        try:
            job = queues.data_out.get(block=True, timeout=1)

            logger.info('dataset=%s rse=%s' % (job['destinationDblock'], job['ddmEndPointOut'].split(',')[0]))

            # send_state(job, args, 'running')  # not necessary to send job update at this point?

            if _stage_out_all(job, args):
                queues.finished_data_out.put(job)
            else:
                queues.failed_data_out.put(job)

        except Queue.Empty:
            continue


def prepare_log(job, tarball_name):
    log = logger.getChild(job.jobid)
    log.info('preparing log file')

    input_files = job['inFiles'].split(',')
    output_files = job['outFiles'].split(',')
    force_exclude = ['geomDB', 'sqlite200']

    with tarfile.open(name=os.path.join(job.workdir, job['logFile']),
                      mode='w:gz',
                      dereference=True) as log_tar:
        for _file in list(set(os.listdir(job.workdir)) - set(input_files) - set(output_files) - set(force_exclude)):
            if os.path.exists(os.path.join(job.workdir, _file)):
                logging.debug('adding to log: %s' % _file)
                log_tar.add(os.path.join(job.workdir, _file),
                            arcname=os.path.join(tarball_name, _file))

    return {'scope': job['scopeLog'],
            'name': job['logFile'],
            'guid': job['logGUID'],
            'bytes': os.stat(os.path.join(job.workdir, job['logFile'])).st_size}


def _stage_out_all(job, args):
    """
    Order stage-out of all output files and the log file, or only the log file.

    :param job:
    :param args:
    :return:
    """

    log = logger.getChild(job.jobid)
    outputs = {}

    if job['stageout'] == 'log':
        log.info('will stage-out log file')
    else:
        log.info('will stage-out all output files and log file')
        out_files = job.get('metaData', {}).get('files', {}).get('output', {})
        for f in out_files:
            subfile = f['subFiles'][0]
            # TODO source/destination?
            outputs[subfile['name']] = {'scope': job['scopeOut'],
                                        'name': subfile['name'],
                                        'guid': subfile['file_guid'],
                                        'bytes': subfile['file_size']}
        else:
            log.warning('Job object does not contain a job report (payload failed?) - will only stage-out log file')
    outputs['%s:%s' % (job['scopeLog'], job['logFile'])] = prepare_log(job, 'tarball_PandaJob_%s_%s' %
                                                                       (job.jobid, args.queue))

    fileinfodict = {}
    failed = False
    try:
        stage_client = StageOutClient(logger=log)
        for outfile in outputs:
            if outfile not in job['outFiles']:
                continue
            output = None
            output = stage_client.transfer(outputs[outfile])

            failed = not output or output['status'] == 'failed':
            if failed:
                break
# 
#        if summary is not None:
#            outputs[outfile]['pfn'] = summary['%s:%s' % (outputs[outfile]['scope'], outputs[outfile]['name'])]['pfn']
#            outputs[outfile]['adler32'] = summary['%s:%s' % (outputs[outfile]['scope'],
#                                                             outputs[outfile]['name'])]['adler32']
#
#            filedict = {'guid': outputs[outfile]['guid'],
#                        'fsize': outputs[outfile]['bytes'],
#                        'adler32': outputs[outfile]['adler32'],
#                        'surl': outputs[outfile]['pfn']}
#            fileinfodict[outputs[outfile]['name']] = filedict
#        else:
#            failed = True
    except Exception as error:
        logger.debug('Error: %s' % error)


    if failed:
        # set error code + message
        job['pilotErrorCodes'], job['pilotErrorDiags'] = errors.add_error_code(errors.STAGEOUTFAILED)
        job['state'] = "failed"
        log.warning('stage-out failed')  # with error: %d, %s (setting job state to failed)' %
        # log.warning('stage-out failed with error: %d, %s (setting job state to failed)' %
        #  (job['pilotErrorCode'], job['pilotErrorDiag']))
        # send_state(job, args, 'failed')
        return False
    else:
        log.info('stage-out finished correctly (setting job state to finished)')
        job['fileinfodict'] = fileinfodict
        job['state'] = "finished"

        # send final server update since all transfers have finished correctly
        # send_state(job, args, 'finished', xml=dumps(fileinfodict))
        return True


def queue_monitoring(queues, traces, args):
    """
    Monitoring of Data queues.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        # wait a second
        if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility reasons
            break

        # monitor the failed_data_in queue
        try:
            job = queues.failed_data_in.get(block=True, timeout=1)
        except Queue.Empty:
            pass
        else:
            # stage-out log file then add the job to the failed_jobs queue
            job['stageout'] = "log"
            if not _stage_out_all(job, args):
                logger.info("job %s failed during stage-in and stage-out of log, adding job object to failed_data_outs "
                            "queue" % job.jobid)
                queues.failed_data_out.put(job)
            else:
                logger.info("job %s failed during stage-in, adding job object to failed_jobs queue" % job.jobid)
                queues.failed_jobs.put(job)

        # monitor the finished_data_out queue
        try:
            job = queues.finished_data_out.get(block=True, timeout=1)
        except Queue.Empty:
            pass
        else:
            # use the payload/transform exitCode from the job report if it exists
            if 'exitCode' in job:
                exit_code = job['exitCode']
            else:  # ignore it
                exit_code = 0

            if ('transExitCode' in job and job['transExitCode'] == 0) and (exit_code == 0):
                logger.info('finished stage-out for finished payload, adding job to finished_jobs queue')
                queues.finished_jobs.put(job)
            else:
                logger.info('finished stage-out (of log) for failed payload')
                queues.failed_jobs.put(job)

        # monitor the failed_data_out queue
        try:
            job = queues.failed_data_out.get(block=True, timeout=1)
        except Queue.Empty:
            pass
        else:
            logger.info("job %s failed during stage-out, adding job object to failed_jobs queue" % job.jobid)
            queues.failed_jobs.put(job)
