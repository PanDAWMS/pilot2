#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2018
# - Wen Guan, wen.guan@cern.ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018

import copy
import Queue
import json
import os
import subprocess
import tarfile
import time

from contextlib import closing  # for Python 2.6 compatibility - to fix a problem with tarfile

from pilot.api.data import StageInClient, StageOutClient
from pilot.control.job import send_state
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import ExcThread, PilotException
from pilot.util.auxiliary import get_logger
from pilot.util.config import config
from pilot.util.constants import PILOT_PRE_STAGEIN, PILOT_POST_STAGEIN, PILOT_PRE_STAGEOUT, PILOT_POST_STAGEOUT
from pilot.util.container import execute
from pilot.util.filehandling import find_executable, get_guid, get_local_file_size
from pilot.util.timing import add_to_pilot_timing

import logging

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def control(queues, traces, args):

    targets = {'copytool_in': copytool_in, 'copytool_out': copytool_out, 'queue_monitoring': queue_monitoring}
    threads = [ExcThread(bucket=Queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in targets.items()]

    [thread.start() for thread in threads]

    # if an exception is thrown, the graceful_stop will be set by the ExcThread class run() function
    while not args.graceful_stop.is_set():
        for thread in threads:
            bucket = thread.get_bucket()
            try:
                exc = bucket.get(block=False)
            except Queue.Empty:
                pass
            else:
                exc_type, exc_obj, exc_trace = exc
                logger.warning("thread \'%s\' received an exception from bucket: %s" % (thread.name, exc_obj))

                # deal with the exception
                # ..

            thread.join(0.1)
            time.sleep(0.1)


def prepare_for_container(workdir):
    """
    Prepare the executable for using a container.
    The function adds necessary setup variables to the executable.
    WARNING: CURRENTLY ATLAS SPECIFIC

    :param workdir: working directory of the job (string).
    :return: setup string to be prepended to the executable.
    """

    from pilot.user.atlas.setup import get_asetup
    return get_asetup(asetup=False) + 'lsetup rucio;'


def use_container(cmd):
    """
    Should the pilot use a container for the stage-in/out?

    :param cmd: middleware command, used to determine if the container should be used or not (string).
    :return: Boolean.
    """

    usecontainer = False
    if config.Container.allow_container == "False":
        logger.info('container usage is not allowed by pilot config')
    else:
        # if the middleware is available locally, do not use container
        if find_executable(cmd) == "":
            usecontainer = True
            logger.info('command %s is not available locally, will attempt to use container' % cmd)
        else:
            logger.info('command %s is available locally, no need to use container' % cmd)

    return usecontainer


def _call(args, executable, job, cwd=os.getcwd(), logger=logger):
    try:
        # for containers, we can not use a list
        usecontainer = use_container(executable[1])
        if usecontainer:
            executable = ' '.join(executable)
            executable = prepare_for_container(job.workdir) + executable

        process = execute(executable, workdir=job.workdir, returnproc=True,
                          usecontainer=usecontainer, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, job=job)

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

    # in case of problems, try to identify the error (and set it in the job object)
    if stderr != "":
        # rucio stage-out error
        if "Operation timed out" in stderr:
            logger.warning('rucio stage-in error identified - problem with local storage, stage-in timed out')
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINTIMEOUT)

    if exit_code == 0:
        return True
    else:
        return False


def _stage_in(args, job):
    """
        :return: True in case of success
    """

    log = get_logger(job.jobid)

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEIN, time.time())

    try:
        client = StageInClient(job.infosys, logger=log)
        kwargs = dict(workdir=job.workdir, cwd=job.workdir, usecontainer=False, job=job)
        client.transfer(job.indata, activity='pr', **kwargs)
    except Exception, error:
        log.error('Failed to stage-in: error=%s' % error)
        #return False

    log.info('Summary of transferred files:')
    for e in job.indata:
        log.info(" -- lfn=%s, status_code=%s, status=%s" % (e.lfn, e.status_code, e.status))

    log.info("stage-in finished")

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_POST_STAGEIN, time.time())

    remain_files = [e for e in job.indata if e.status not in ['remote_io', 'transferred', 'no_transfer']]

    return not remain_files


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
            logger.info('Test job.infosys: queuedata.acopytools=%s' % job.infosys.queuedata.acopytools)

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

            if _stage_out_new(job, args):
                queues.finished_data_out.put(job)
            else:
                queues.failed_data_out.put(job)

        except Queue.Empty:
            continue


def prepare_log(job, logfile, tarball_name):
    log = get_logger(job.jobid)
    log.info('preparing log file')

    input_files = [e.lfn for e in job.indata]
    output_files = [e.lfn for e in job.outdata]
    force_exclude = ['geomDB', 'sqlite200']

    # perform special cleanup (user specific) prior to log file creation
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], -1)
    user.remove_redundant_files(job.workdir)

    with closing(tarfile.open(name=os.path.join(job.workdir, logfile.lfn), mode='w:gz', dereference=True)) as log_tar:
        for _file in list(set(os.listdir(job.workdir)) - set(input_files) - set(output_files) - set(force_exclude)):
            if os.path.exists(os.path.join(job.workdir, _file)):
                logging.debug('adding to log: %s' % _file)
                log_tar.add(os.path.join(job.workdir, _file),
                            arcname=os.path.join(tarball_name, _file))

    return {'scope': logfile.scope,
            'name': logfile.lfn,
            'guid': logfile.guid,
            'bytes': os.stat(os.path.join(job.workdir, logfile.lfn)).st_size}


def _stage_out(args, outfile, job):  ### TO BE DEPRECATED
    log = get_logger(job.jobid)

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEOUT, time.time())

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
        usecontainer = use_container(executable[1])
        if usecontainer:
            executable = ' '.join(executable)
            executable = prepare_for_container(job.workdir) + executable
        process = execute(executable, workdir=job.workdir, returnproc=True, job=job,
                          usecontainer=usecontainer, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=job.workdir)

        # process = subprocess.Popen(executable,
        #                            bufsize=-1,
        #                            stdout=subprocess.PIPE,
        #                            stderr=subprocess.PIPE,
        #                            cwd=job.workdir)
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

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_POST_STAGEOUT, time.time())

    # in case of problems, try to identify the error (and set it in the job object)
    if err != "":
        # rucio stage-out error
        if "Operation timed out" in err:
            log.warning('rucio stage-out error identified - problem with local storage, stage-out timed out')
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTTIMEOUT)

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


def _do_stageout(job, xdata, activity, title):
    """
        :return: True in case of success transfers
        :raise: PilotException in case of controlled error
    """

    log = get_logger(job.jobid)

    log.info('prepare to stage-out %s files' % title)

    error = None

    client = StageOutClient(job.infosys, logger=log)
    kwargs = dict(workdir=job.workdir, cwd=job.workdir, usecontainer=False, job=job)

    try:
        # check if file exists before actual processing
        # populate filesize if need
        for fspec in xdata:
            pfn = getattr(fspec, 'pfn', None) or os.path.join(job.workdir, fspec.lfn)
            if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                msg = "Error: output pfn file does not exist: %s" % pfn
                log.error(msg)
                raise PilotException(msg, code=ErrorCodes.MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")
            if not fspec.filesize:
                fspec.filesize = os.path.getsize(pfn)
            fspec.surl = pfn
            fspec.activity = activity

        client.transfer(xdata, activity, **kwargs)

    except PilotException, error:
        import traceback
        log.error(traceback.format_exc())
    except Exception, e:
        import traceback
        log.error(traceback.format_exc())
        error = PilotException("stageOut failed with error=%s" % e, code=ErrorCodes.STAGEOUTFAILED)

    log.info('Summary of transferred files:')
    for e in xdata:
        log.info(" -- lfn=%s, status_code=%s, status=%s" % (e.lfn, e.status_code, e.status))

    if error:
        log.error('Failed to stage-out %s file(s): error=%s' % (error, title))
        raise error

    remain_files = [e for e in xdata if e.status not in ['transferred']]

    return not remain_files


def _stage_out_new(job, args):
    """
    Stage-out of all output files.
    If job.stageout=log then only log files will be transferred.

    :param job: job object
    :param args:
    :return: True in case of success
    """

    log = get_logger(job.jobid)

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEOUT, time.time())

    is_success = True
    if job.stageout != 'log':  ## do stage-out output files
        if not _do_stageout(job, job.outdata, ['pw', 'w'], 'output'):
            is_success = False
            log.warning('transfer of output file(s) failed')

    if job.stageout in ['log', 'all'] and job.logdata:  ## do stage-out log files
        # prepare log file
        # consider only 1st available log file
        logfile = job.logdata[0]
        r = prepare_log(job, logfile, 'tarball_PandaJob_%s_%s' % (job.jobid, job.infosys.pandaqueue))
        logfile.filesize = r['bytes']  ## FIX ME LATER: do simplify prepare_log function

        if not _do_stageout(job, [logfile], ['pl', 'pw', 'w'], 'log'):
            is_success = False
            log.warning('log transfer failed')

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_POST_STAGEOUT, time.time())

    # generate fileinfo details to be send to Panda
    fileinfo = {}
    for e in job.outdata + job.logdata:
        if e.status in ['transferred']:
            fileinfo[e.lfn] = {'guid': e.guid, 'fsize': e.filesize,
                               'adler32': e.checksum.get('adler32'),
                               'surl': e.turl}

    job.fileinfo = fileinfo

    if not is_success:
        # set error code + message (a more precise error code might have been set already)
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTFAILED)
        job.state = "failed"
        log.warning('stage-out failed')  # with error: %d, %s (setting job state to failed)' %
        # log.warning('stage-out failed with error: %d, %s (setting job state to failed)' %
        #  (job['pilotErrorCode'], job['pilotErrorDiag']))
        # send_state(job, args, 'failed')
        return False

    log.info('stage-out finished correctly')

    if not job.state:  # is the job state already set? if so, don't change the state
        job.state = "finished"

    # send final server update since all transfers have finished correctly
    # send_state(job, args, 'finished', xml=dumps(fileinfodict))

    return is_success


def _stage_out_all(job, args):  ### TO BE DEPRECATED
    """
    Order stage-out of all output files and the log file, or only the log file.

    :param job:
    :param args:
    :return: True in case of success
    """

    log = get_logger(job.jobid)
    outputs = {}

    if job.stageout == 'log':
        log.info('will stage-out log file')
    else:
        log.info('will stage-out all output files and log file')
        if job.metadata:
            scopes = dict([e.lfn, e.scope] for e in job.outdata)  # quick hack: to be properly implemented later
            # extract output files from the job report, in case the trf has created additional (overflow) files
            for f in job.metadata['files']['output']:
                outputs[f['subFiles'][0]['name']] = {'scope': scopes.get(f['subFiles'][0]['name'],
                                                                         job.scopeout.split(',')[0]),  # [0]? a bug?
                                                     'name': f['subFiles'][0]['name'],
                                                     'guid': f['subFiles'][0]['file_guid'],
                                                     'bytes': f['subFiles'][0]['file_size']}
        elif job.is_build_job():
            # scopes = dict([e.lfn, e.scope] for e in job.outdata)  # quick hack: to be properly implemented later
            for f in job.outdata:  # should be only one output file
                # is the metadata set?
                if f.guid == '':
                    # ok to generate GUID for .lib. file
                    f.guid = get_guid()
                    log.info('generated guid for lib file: %s' % f.guid)
                # is the file size set?
                if f.filesize == 0:
                    f.filesize = get_local_file_size(os.path.join(job.workdir, f.lfn))
                    if f.filesize:
                        log.info('set file size for %s to %d B' % (f.lfn, f.filesize))
                outputs[f.lfn] = {'scope': f.scope, 'name': f.lfn, 'guid': f.guid, 'bytes': f.filesize}
            log.info('outputs=%s' % str(outputs))
        else:
            log.warning('job object does not contain a job report (payload failed?) and is not a build job '
                        '- will only stage-out log file')

    fileinfodict = {}
    failed = False

    # stage-out output files first, wait with log
    for outfile in outputs:
        status = single_stage_out(args, job, outputs[outfile], fileinfodict)
        if not status:
            failed = True
            log.warning('transfer of output file(s) failed')

    # proceed with log transfer
    # consider only 1st available log file
    if job.logdata:
        logfile = job.logdata[0]
        key = '%s:%s' % (logfile.scope, logfile.lfn)
        outputs[key] = prepare_log(job, logfile, 'tarball_PandaJob_%s_%s' % (job.jobid, args.queue))

        status = single_stage_out(args, job, outputs[key], fileinfodict)
        if not status:
            failed = True
            log.warning('log transfer failed')

    job.fileinfo = fileinfodict
    if failed:
        # set error code + message (a more precise error code might have been set already)
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
    log = get_logger(job.jobid)

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
            if not _stage_out_new(job, args):
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
