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

import copy
import Queue
import json
import os
import subprocess
import tarfile
import threading
import time

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
    log = logger.getChild(job.jobid)

    os.environ['RUCIO_LOGGING_FORMAT'] = '{0}%(asctime)s %(levelname)s [%(message)s]'
    if not _call(args,
                 ['/usr/bin/env',
                  'rucio', '-v', 'download',
                  '--no-subdir',
                  '--rse', job.ddmendpointin,
                  '%s:%s' % (job.scopein, job.infiles)],  # notice the bug here, infiles might be a ,-separated str
                 cwd=job.workdir,
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

            logger.info('Test job.infosys: queuedata.copytools=%s' % job.infosys.queuedata.copytools)

            if _stage_in(args, job):
                queues.finished_data_in.put(job)
            else:
                logger.warning('stage-in failed, adding job object to failed_data_in queue')
                queues.failed_data_in.put(job)
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINFAILED)
                job.state = "failed"
                # send_state(job, args, 'failed')

        except Queue.Empty:
            continue


def copytool_out(queues, traces, args):

    while not args.graceful_stop.is_set():
        try:
            job = queues.data_out.get(block=True, timeout=1)

            logger.info('dataset=%s rse=%s' % (job.destinationdblock, job.ddmendpointout.split(',')[0]))

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

    input_files = job.infiles.split(',')
    output_files = job.outfiles.split(',')
    force_exclude = ['geomDB', 'sqlite200']

    # perform special cleanup (user specific) prior to log file creation
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], -1)
    user.remove_redundant_files(job.workdir)

    with tarfile.open(name=os.path.join(job.workdir, job.logfile),
                      mode='w:gz',
                      dereference=True) as log_tar:
        for _file in list(set(os.listdir(job.workdir)) - set(input_files) - set(output_files) - set(force_exclude)):
            if os.path.exists(os.path.join(job.workdir, _file)):
                logging.debug('adding to log: %s' % _file)
                log_tar.add(os.path.join(job.workdir, _file),
                            arcname=os.path.join(tarball_name, _file))

    return {'scope': job.scopelog,
            'name': job.logfile,
            'guid': job.logguid,
            'bytes': os.stat(os.path.join(job.workdir, job.logfile)).st_size}


def _stage_out(args, outfile, job):
    log = logger.getChild(job.jobid)

    log.info('will stage-out: %s' % outfile)

    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'
    executable = ['/usr/bin/env',
                  'rucio', '-v', 'upload',
                  '--summary', '--no-register',
                  '--guid', outfile['guid'],
                  '--rse', job.ddmendpointout.split(',')[0],
                  '--scope', outfile['scope'],
                  outfile['name']]

    try:
        process = subprocess.Popen(executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   cwd=job.workdir)
    except Exception as e:
        log.error('could not execute: %s' % str(e))
        return None

    log.info('started -- pid=%s executable=%s' % (process.pid, executable))

    breaker = False
    exit_code = None
    first = True
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
        if first and not exit_code:
            log.info('running -- pid=%s exit_code=%s' % (process.pid, exit_code))
            first = False
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
    path = os.path.join(job.workdir, 'rucio_upload.json')
    if not os.path.exists(path):
        log.warning('no such file: %s' % path)
        return None
    else:
        with open(path, 'rb') as summary_file:
            summary = json.load(summary_file)

    return summary


def _stage_out_all(job, args):
    """
    Order stage-out of all output files and the log file, or only the log file.

    :param job:
    :param args:
    :return:
    """

    log = logger.getChild(job.jobid)
    outputs = {}

    if job.stageout == 'log':
        log.info('will stage-out log file')
    else:
        log.info('will stage-out all output files and log file')
        if job.metadata:
            for f in job.metadata['files']['output']:
                outputs[f['subFiles'][0]['name']] = {'scope': job.scopeout,
                                                     'name': f['subFiles'][0]['name'],
                                                     'guid': f['subFiles'][0]['file_guid'],
                                                     'bytes': f['subFiles'][0]['file_size']}
        else:
            log.warning('Job object does not contain a job report (payload failed?) - will only stage-out log file')

    fileinfodict = {}
    failed = False

    # stage-out output files first, wait with log
    for outfile in outputs:
        status = single_stage_out(args, job, outputs[outfile], fileinfodict)
        if not status:
            failed = True
            log.warning('transfer of output file(s) failed')

    # proceed with log transfer
    outputs['%s:%s' % (job.scopelog, job.logfile)] = prepare_log(job, 'tarball_PandaJob_%s_%s' %
                                                                 (job.jobid, args.queue))
    status = single_stage_out(args, job, outputs['%s:%s' % (job.scopelog, job.logfile)], fileinfodict)
    if not status:
        failed = True
        log.warning('log transfer failed')

    job.fileinfo = fileinfodict
    if failed:
        # set error code + message
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTFAILED)
        job.state = "failed"
        log.warning('stage-out failed')  # with error: %d, %s (setting job state to failed)' %
        # log.warning('stage-out failed with error: %d, %s (setting job state to failed)' %
        #  (job['pilotErrorCode'], job['pilotErrorDiag']))
        # send_state(job, args, 'failed')
        return False
    else:
        log.info('stage-out finished correctly')
        # is the job state already set? if so, don't change the state
        if 'state' not in job:
            job.state = "finished"

        # send final server update since all transfers have finished correctly
        # send_state(job, args, 'finished', xml=dumps(fileinfodict))
        return True


def single_stage_out(args, job, outputs_outfile, fileinfodict):
    """
    Perform stage-out for single file and populate the outputs and fileinfodict dictionaries.

    :param args: pilot arguments
    :param job: job object
    :param outputs_output: output file dictionary entry
    :param fileinfodict: file metadata dictionary
    :return: status (boolean)
    """

    status = True
    log = logger.getChild(job.jobid)

    # this doesn't work since scope is added above, but scope is not present in outFiles
    # if outfile not in job['outFiles']:
    #     continue
    summary = _stage_out(args, outputs_outfile, job)
    log.info('stage-out finished for %s (summary=%s)' % (outputs_outfile, str(summary)))

    if summary is not None:
        outputs_outfile['pfn'] = summary['%s:%s' % (outputs_outfile['scope'], outputs_outfile['name'])]['pfn']
        outputs_outfile['adler32'] = summary['%s:%s' % (outputs_outfile['scope'],
                                                        outputs_outfile['name'])]['adler32']

        filedict = {'guid': outputs_outfile['guid'],
                    'fsize': outputs_outfile['bytes'],
                    'adler32': outputs_outfile['adler32'],
                    'surl': outputs_outfile['pfn']}
        fileinfodict[outputs_outfile['name']] = filedict
    else:
        status = False

    return status


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
            job.stageout = "log"
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
            if job.transexitcode == 0 and job.exitcode == 0:
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
