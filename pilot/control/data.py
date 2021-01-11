#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2020
# - Wen Guan, wen.guan@cern.ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018

import copy as objectcopy
import os
import subprocess
import time

try:
    import Queue as queue  # noqa: N813
except Exception:
    import queue  # Python 3

from pilot.api.data import StageInClient, StageOutClient
from pilot.api.es_data import StageInESClient
from pilot.control.job import send_state
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import ExcThread, PilotException, LogFileCreationFailure
from pilot.util.config import config
from pilot.util.auxiliary import set_pilot_state, check_for_final_server_update  #, abort_jobs_in_queues
from pilot.util.common import should_abort
from pilot.util.constants import PILOT_PRE_STAGEIN, PILOT_POST_STAGEIN, PILOT_PRE_STAGEOUT, PILOT_POST_STAGEOUT, LOG_TRANSFER_IN_PROGRESS,\
    LOG_TRANSFER_DONE, LOG_TRANSFER_NOT_DONE, LOG_TRANSFER_FAILED, SERVER_UPDATE_RUNNING, MAX_KILL_WAIT_TIME
from pilot.util.container import execute
from pilot.util.filehandling import remove, get_local_file_size
from pilot.util.processes import threads_aborted
from pilot.util.queuehandling import declare_failed_by_kill, put_in_queue
from pilot.util.timing import add_to_pilot_timing
from pilot.util.tracereport import TraceReport
import pilot.util.middleware

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def control(queues, traces, args):

    targets = {'copytool_in': copytool_in, 'copytool_out': copytool_out, 'queue_monitoring': queue_monitoring}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in list(targets.items())]  # Python 2/3

    [thread.start() for thread in threads]

    # if an exception is thrown, the graceful_stop will be set by the ExcThread class run() function
    while not args.graceful_stop.is_set():
        for thread in threads:
            bucket = thread.get_bucket()
            try:
                exc = bucket.get(block=False)
            except queue.Empty:
                pass
            else:
                exc_type, exc_obj, exc_trace = exc
                logger.warning("thread \'%s\' received an exception from bucket: %s" % (thread.name, exc_obj))

                # deal with the exception
                # ..

            thread.join(0.1)
            time.sleep(0.1)

        time.sleep(0.5)

    logger.debug('data control ending since graceful_stop has been set')
    if args.abort_job.is_set():
        if traces.pilot['command'] == 'aborting':
            logger.warning('jobs are aborting')
        elif traces.pilot['command'] == 'abort':
            logger.warning('data control detected a set abort_job (due to a kill signal)')
            traces.pilot['command'] = 'aborting'

            # find all running jobs and stop them, find all jobs in queues relevant to this module
            #abort_jobs_in_queues(queues, args.signal)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[data] control thread has finished')


def skip_special_files(job):
    """
    Consult user defined code if any files should be skipped during stage-in.
    ATLAS code will skip DBRelease files e.g. as they should already be available in CVMFS.

    :param job: job object.
    :return:
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
    try:
        user.update_stagein(job)
    except Exception as e:
        logger.warning('caught exception: %s' % e)


def update_indata(job):
    """
    In case file were marked as no_transfer files, remove them from stage-in.

    :param job: job object.
    :return:
    """

    toberemoved = []
    for fspec in job.indata:
        if fspec.status == 'no_transfer':
            toberemoved.append(fspec)
    for fspec in toberemoved:
        logger.info('removing fspec object (lfn=%s) from list of input files' % fspec.lfn)
        job.indata.remove(fspec)


def get_trace_report_variables(job, label='stage-in'):
    """
    Get some of the variables needed for creating the trace report.

    :param job: job object
    :param label: 'stage-[in|out]' (string).
    :return: event_type (string), localsite (string), remotesite (string).
    """

    event_type = "get_sm" if label == 'stage-in' else "put_sm"
    if job.is_analysis():
        event_type += "_a"
    data = job.indata if label == 'stage-in' else job.outdata
    localsite = remotesite = get_rse(data)

    return event_type, localsite, remotesite


def create_trace_report(job, label='stage-in'):
    """
    Create the trace report object.

    :param job: job object.
    :param label: 'stage-[in|out]' (string).
    :return: trace report object.
    """

    event_type, localsite, remotesite = get_trace_report_variables(job, label=label)
    trace_report = TraceReport(pq=os.environ.get('PILOT_SITENAME', ''), localSite=localsite, remoteSite=remotesite,
                               dataset="", eventType=event_type)
    trace_report.init(job)

    return trace_report


def _stage_in(args, job):
    """
        :return: True in case of success
    """

    # tested ok:
    #logger.info('testing sending SIGUSR1')
    #import signal
    #os.kill(os.getpid(), signal.SIGUSR1)

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEIN, time.time(), args)

    # any DBRelease files should not be staged in
    skip_special_files(job)

    # now that the trace report has been created, remove any files that are not to be transferred (DBRelease files) from the indata list
    update_indata(job)

    label = 'stage-in'

    # should stage-in be done by a script (for containerisation) or by invoking the API (ie classic mode)?
    use_container = pilot.util.middleware.use_middleware_script(job.infosys.queuedata.container_type.get("middleware"))
    if use_container:
        logger.info('stage-in will be done by a script')
        try:
            eventtype, localsite, remotesite = get_trace_report_variables(job, label=label)
            pilot.util.middleware.containerise_middleware(job, job.indata, args.queue, eventtype, localsite, remotesite,
                                                          job.infosys.queuedata.container_options, args.input_dir,
                                                          label=label, container_type=job.infosys.queuedata.container_type.get("middleware"))
        except PilotException as e:
            logger.warning('stage-in containerisation threw a pilot exception: %s' % e)
        except Exception as e:
            import traceback
            logger.warning('stage-in containerisation threw an exception: %s' % e)
            logger.error(traceback.format_exc())
    else:
        try:
            logger.info('stage-in will not be done in a container')

            # create the trace report
            trace_report = create_trace_report(job, label=label)

            if job.is_eventservicemerge:
                client = StageInESClient(job.infosys, logger=logger, trace_report=trace_report)
                activity = 'es_events_read'
            else:
                client = StageInClient(job.infosys, logger=logger, trace_report=trace_report)
                activity = 'pr'
            use_pcache = job.infosys.queuedata.use_pcache
            kwargs = dict(workdir=job.workdir, cwd=job.workdir, usecontainer=False, use_pcache=use_pcache, use_bulk=False,
                          input_dir=args.input_dir, use_vp=job.use_vp, catchall=job.infosys.queuedata.catchall)
            client.prepare_sources(job.indata)
            client.transfer(job.indata, activity=activity, **kwargs)
        except PilotException as error:
            import traceback
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            msg = errors.format_diagnostics(error.get_error_code(), error_msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code(), msg=msg)
        except Exception as error:
            logger.error('failed to stage-in: error=%s' % error)

    logger.info('summary of transferred files:')
    for e in job.indata:
        status = e.status if e.status else "(not transferred)"
        logger.info(" -- lfn=%s, status_code=%s, status=%s" % (e.lfn, e.status_code, status))

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_POST_STAGEIN, time.time(), args)

    remain_files = [e for e in job.indata if e.status not in ['remote_io', 'transferred', 'no_transfer']]
    logger.info("stage-in finished") if not remain_files else logger.info("stage-in failed")

    return not remain_files


def get_rse(data, lfn=""):
    """
    Return the ddmEndPoint corresponding to the given lfn.
    If lfn is not provided, the first ddmEndPoint will be returned.

    :param data: FileSpec list object.
    :param lfn: local file name (string).
    :return: rse (string)
    """

    rse = ""

    if lfn == "":
        try:
            return data[0].ddmendpoint
        except Exception as e:
            logger.warning("exception caught: %s" % e)
            logger.warning("end point is currently unknown")
            return "unknown"

    for fspec in data:
        if fspec.lfn == lfn:
            rse = fspec.ddmendpoint

    if rse == "":
        logger.warning("end point is currently unknown")
        rse = "unknown"

    return rse


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

        tmp_executable = objectcopy.deepcopy(executable)

        tmp_executable += ['--dir', f['destination']]
        tmp_executable.append('%s:%s' % (f['scope'],
                                         f['name']))
        process = subprocess.Popen(tmp_executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        f['errno'] = 2
        while True:
            time.sleep(0.5)
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

        tmp_executable = objectcopy.deepcopy(executable)

        tmp_executable += ['--rse', f['rse']]

        if 'no_register' in list(f.keys()) and f['no_register']:  # Python 2/3
            tmp_executable += ['--no-register']

        if 'summary' in list(f.keys()) and f['summary']:  # Python 2/3
            tmp_executable += ['--summary']

        if 'lifetime' in list(f.keys()):  # Python 2/3
            tmp_executable += ['--lifetime', str(f['lifetime'])]

        if 'guid' in list(f.keys()):  # Python 2/3
            tmp_executable += ['--guid', f['guid']]

        if 'attach' in list(f.keys()):  # Python 2/3
            tmp_executable += ['--scope', f['scope'], '%s:%s' % (f['attach']['scope'], f['attach']['name']), f['file']]
        else:
            tmp_executable += ['--scope', f['scope'], f['file']]

        process = subprocess.Popen(tmp_executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        f['errno'] = 2
        while True:
            time.sleep(0.5)
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
    """
    Call the stage-in function and put the job object in the proper queue.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            # abort if kill signal arrived too long time ago, ie loop is stuck
            current_time = int(time.time())
            if args.kill_time and current_time - args.kill_time > MAX_KILL_WAIT_TIME:
                logger.warning('loop has run for too long time after first kill signal - will abort')
                break

            # extract a job to stage-in its input
            job = queues.data_in.get(block=True, timeout=1)
            # place it in the current stage-in queue (used by the jobs' queue monitoring)
            if job:
                put_in_queue(job, queues.current_data_in)

            # ready to set the job in running state
            send_state(job, args, 'running')

            # note: when sending a state change to the server, the server might respond with 'tobekilled'
            if job.state == 'failed':
                logger.warning('job state is \'failed\' - order log transfer and abort copytool_in()')
                job.stageout = 'log'  # only stage-out log file
                put_in_queue(job, queues.data_out)
                break

            os.environ['SERVER_UPDATE'] = SERVER_UPDATE_RUNNING

            if args.abort_job.is_set():
                traces.pilot['command'] = 'abort'
                logger.warning('copytool_in detected a set abort_job pre stage-in (due to a kill signal)')
                declare_failed_by_kill(job, queues.failed_data_in, args.signal)
                break

            if _stage_in(args, job):
                if args.abort_job.is_set():
                    traces.pilot['command'] = 'abort'
                    logger.warning('copytool_in detected a set abort_job post stage-in (due to a kill signal)')
                    declare_failed_by_kill(job, queues.failed_data_in, args.signal)
                    break

                put_in_queue(job, queues.finished_data_in)
                # remove the job from the current stage-in queue
                _job = queues.current_data_in.get(block=True, timeout=1)
                if _job:
                    logger.debug('job %s has been removed from the current_data_in queue' % _job.jobid)

                # now create input file metadata if required by the payload
                if config.Payload.executor_type.lower() != 'raythena':
                    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
                    user = __import__('pilot.user.%s.metadata' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
                    file_dictionary = get_input_file_dictionary(job.indata)
                    xml = user.create_input_file_metadata(file_dictionary, job.workdir)
                    logger.info('created input file metadata:\n%s' % xml)
            else:
                logger.warning('stage-in failed, adding job object to failed_data_in queue')
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINFAILED)
                set_pilot_state(job=job, state="failed")
                traces.pilot['error_code'] = job.piloterrorcodes[0]
                put_in_queue(job, queues.failed_data_in)
                # do not set graceful stop if pilot has not finished sending the final job update
                # i.e. wait until SERVER_UPDATE is DONE_FINAL
                check_for_final_server_update(args.update_server)
                args.graceful_stop.set()

        except queue.Empty:
            continue

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[data] copytool_in thread has finished')


def copytool_out(queues, traces, args):
    """
    Main stage-out thread.
    Perform stage-out as soon as a job object can be extracted from the data_out queue.

    :param queues: pilot queues object.
    :param traces: pilot traces object.
    :param args: pilot args object.
    :return:
    """

    cont = True
    logger.debug('entering copytool_out loop')
    if args.graceful_stop.is_set():
        logger.debug('graceful_stop already set')

    processed_jobs = []
    while cont:

        time.sleep(0.5)

        # abort if kill signal arrived too long time ago, ie loop is stuck
        current_time = int(time.time())
        if args.kill_time and current_time - args.kill_time > MAX_KILL_WAIT_TIME:
            logger.warning('loop has run for too long time after first kill signal - will abort')
            break

        # check for abort, print useful messages and include a 1 s sleep
        abort = should_abort(args, label='data:copytool_out')
        try:
            job = queues.data_out.get(block=True, timeout=1)
            if job:
                # hack to prevent stage-out to be called more than once for same job object (can apparently happen
                # in multi-output jobs)
                # should not be necessary unless job object is added to queues.data_out more than once - check this
                # for multiple output files
                if processed_jobs:
                    if is_already_processed(queues, processed_jobs):
                        continue

                logger.info('will perform stage-out for job id=%s' % job.jobid)

                if args.abort_job.is_set():
                    traces.pilot['command'] = 'abort'
                    logger.warning('copytool_out detected a set abort_job pre stage-out (due to a kill signal)')
                    declare_failed_by_kill(job, queues.failed_data_out, args.signal)
                    break

                if _stage_out_new(job, args):
                    if args.abort_job.is_set():
                        traces.pilot['command'] = 'abort'
                        logger.warning('copytool_out detected a set abort_job post stage-out (due to a kill signal)')
                        #declare_failed_by_kill(job, queues.failed_data_out, args.signal)
                        break

                    #queues.finished_data_out.put(job)
                    processed_jobs.append(job.jobid)
                    put_in_queue(job, queues.finished_data_out)
                    logger.debug('job object added to finished_data_out queue')
                else:
                    #queues.failed_data_out.put(job)
                    put_in_queue(job, queues.failed_data_out)
                    logger.debug('job object added to failed_data_out queue')
            else:
                logger.debug('no returned job - why no exception?')
        except queue.Empty:
            if abort:
                cont = False
                break
            continue

        if abort:
            cont = False
            break

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[data] copytool_out thread has finished')


def is_already_processed(queues, processed_jobs):
    """
    Skip stage-out in case the job has already been processed.
    This should not be necessary so this is a fail-safe but it seems there is a case when a job with multiple output
    files enters the stage-out more than once.

    :param queues: queues object.
    :param processed_jobs: list of already processed jobs.
    :return: True if stage-out queues contain a job object that has already been processed.
    """

    snapshots = list(queues.finished_data_out.queue) + list(queues.failed_data_out.queue)
    jobids = [obj.jobid for obj in snapshots]
    found = False

    for jobid in processed_jobs:
        if jobid in jobids:
            logger.warning('output from job %s has already been staged out' % jobid)
            found = True
            break
    if found:
        time.sleep(5)

    return found


def get_input_file_dictionary(indata):
    """
    Return an input file dictionary.
    Format: {'guid': 'pfn', ..}
    Normally use_turl would be set to True if direct access is used.

    :param indata: list of FileSpec objects.
    :return: file dictionary.
    """

    ret = {}

    for fspec in indata:
        ret[fspec.guid] = fspec.turl if fspec.status == 'remote_io' else fspec.lfn

        # correction for ND and mv
        # in any case use the lfn instead of pfn since there are trf's that have problems with pfn's
        if not ret[fspec.guid]:   # this case never works (turl/lfn is always non empty), deprecated code?
            ret[fspec.guid] = fspec.lfn

    return ret


def filter_files_for_log(directory):
    """
    Create a file list recursi
    :param directory:
    :return:
    """
    filtered_files = []
    maxfilesize = 10
    for root, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            location = os.path.join(root, filename)
            if os.path.exists(location):  # do not include broken links
                if os.path.getsize(location) < maxfilesize:
                    filtered_files.append(location)

    return filtered_files


def create_log(job, logfile, tarball_name, args):
    """

    :param job:
    :param logfile:
    :param tarball_name:
    :raises LogFileCreationFailure: in case of log file creation problem
    :return:
    """

    logger.debug('preparing to create log file')
    pilot_home = os.environ.get('PILOT_HOME', os.getcwd())
    current_dir = os.getcwd()
    if pilot_home != current_dir:
        logger.debug('cd from %s to %s for log creation' % (current_dir, pilot_home))
        os.chdir(pilot_home)

    # perform special cleanup (user specific) prior to log file creation
    if args.cleanup:
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
        user.remove_redundant_files(job.workdir, islooping=errors.LOOPINGJOB in job.piloterrorcodes)
    else:
        logger.debug('user specific cleanup not performed')

    input_files = [e.lfn for e in job.indata]
    output_files = [e.lfn for e in job.outdata]

    # remove any present input/output files before tarring up workdir
    for f in input_files + output_files:
        path = os.path.join(job.workdir, f)
        if os.path.exists(path):
            logger.info('removing file: %s' % path)
            remove(path)

    # rename the workdir for the tarball creation
    newworkdir = os.path.join(os.path.dirname(job.workdir), tarball_name)
    orgworkdir = job.workdir
    logger.debug('renaming %s to %s' % (job.workdir, newworkdir))
    os.rename(job.workdir, newworkdir)
    job.workdir = newworkdir

    fullpath = os.path.join(job.workdir, logfile.lfn)  # /some/path/to/dirname/log.tgz

    logger.info('will create archive %s' % fullpath)
    try:
        #newdirnm = "tarball_PandaJob_%s" % job.jobid
        #tarballnm = "%s.tar.gz" % newdirnm
        #os.rename(job.workdir, newdirnm)
        cmd = "pwd;tar cvfz %s %s --dereference --one-file-system; echo $?" % (fullpath, tarball_name)
        exit_code, stdout, stderr = execute(cmd)

        #with closing(tarfile.open(name=fullpath, mode='w:gz', dereference=True)) as archive:
        #    archive.add(os.path.basename(job.workdir), recursive=True)
    except Exception as e:
        raise LogFileCreationFailure(e)
    else:
        if pilot_home != current_dir:
            logger.debug('cd from %s to %s after log creation' % (pilot_home, current_dir))
            os.chdir(pilot_home)
        logger.debug('stdout = %s' % stdout)

        # verify the size of the log file
        size = get_local_file_size(fullpath)
        if size < 1024:
            logger.warning('log file size too small: %d B' % size)
        else:
            logger.info('log file size: %d B' % size)

    logger.debug('renaming %s back to %s' % (job.workdir, orgworkdir))
    try:
        os.rename(job.workdir, orgworkdir)
    except Exception as e:
        logger.debug('exception caught: %s' % e)
    job.workdir = orgworkdir

    #fullpath = os.path.join(job.workdir, logfile.lfn)  # reset fullpath since workdir has changed since above
    #return {'scope': logfile.scope,
    #        'name': logfile.lfn,
    #        'guid': logfile.guid,
    #        'bytes': os.stat(fullpath).st_size}


def _do_stageout(job, xdata, activity, queue, title, output_dir=''):
    """
    Use the `StageOutClient` in the Data API to perform stage-out.

    :param job: job object.
    :param xdata: list of FileSpec objects.
    :param activity: copytool activity or preferred list of activities to resolve copytools
    :param title: type of stage-out (output, log) (string).
    :param queue: PanDA queue (string).
    :return: True in case of success transfers
    """

    logger.info('prepare to stage-out %d %s file(s)' % (len(xdata), title))
    label = 'stage-out'

    # should stage-in be done by a script (for containerisation) or by invoking the API (ie classic mode)?
    use_container = pilot.util.middleware.use_middleware_script(job.infosys.queuedata.container_type.get("middleware"))
    if use_container:
        logger.info('stage-out will be done by a script')
        try:
            eventtype, localsite, remotesite = get_trace_report_variables(job, label=label)
            pilot.util.middleware.containerise_middleware(job, xdata, queue, eventtype, localsite, remotesite,
                                                          job.infosys.queuedata.container_options, output_dir,
                                                          label=label, container_type=job.infosys.queuedata.container_type.get("middleware"))
        except PilotException as e:
            logger.warning('stage-out containerisation threw a pilot exception: %s' % e)
        except Exception as e:
            logger.warning('stage-out containerisation threw an exception: %s' % e)
    else:
        try:
            logger.info('stage-out will not be done in a container')

            # create the trace report
            trace_report = create_trace_report(job, label=label)

            client = StageOutClient(job.infosys, logger=logger, trace_report=trace_report)
            kwargs = dict(workdir=job.workdir, cwd=job.workdir, usecontainer=False, job=job, output_dir=output_dir,
                          catchall=job.infosys.queuedata.catchall)  #, mode='stage-out')
            # prod analy unification: use destination preferences from PanDA server for unified queues
            if job.infosys.queuedata.type != 'unified':
                client.prepare_destinations(xdata, activity)  ## FIX ME LATER: split activities: for astorages and for copytools (to unify with ES workflow)
            client.transfer(xdata, activity, **kwargs)
        except PilotException as error:
            import traceback
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            msg = errors.format_diagnostics(error.get_error_code(), error_msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code(), msg=msg)
        except Exception:
            import traceback
            logger.error(traceback.format_exc())
            # do not raise the exception since that will prevent also the log from being staged out
            # error = PilotException("stageOut failed with error=%s" % e, code=ErrorCodes.STAGEOUTFAILED)
        else:
            logger.debug('stage-out client completed')

    logger.info('summary of transferred files:')
    for e in xdata:
        if not e.status:
            status = "(not transferred)"
        else:
            status = e.status
        logger.info(" -- lfn=%s, status_code=%s, status=%s" % (e.lfn, e.status_code, status))

    remain_files = [e for e in xdata if e.status not in ['transferred']]
    logger.debug('remain_files=%s' % str(remain_files))
    logger.debug('xdata=%s' % str(xdata))

    return not remain_files


def _stage_out_new(job, args):
    """
    Stage-out of all output files.
    If job.stageout=log then only log files will be transferred.

    :param job: job object.
    :param args: pilot args object.
    :return: True in case of success, False otherwise.
    """

    #logger.info('testing sending SIGUSR1')
    #import signal
    #os.kill(os.getpid(), signal.SIGUSR1)

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEOUT, time.time(), args)

    is_success = True

    if not job.outdata or job.is_eventservice:
        logger.info('this job does not have any output files, only stage-out log file')
        job.stageout = 'log'

    if job.stageout != 'log':  ## do stage-out output files
        if not _do_stageout(job, job.outdata, ['pw', 'w'], args.queue, title='output', output_dir=args.output_dir):
            is_success = False
            logger.warning('transfer of output file(s) failed')

    if job.stageout in ['log', 'all'] and job.logdata:  ## do stage-out log files
        # prepare log file, consider only 1st available log file
        status = job.get_status('LOG_TRANSFER')
        if status != LOG_TRANSFER_NOT_DONE:
            logger.warning('log transfer already attempted')
            return False

        job.status['LOG_TRANSFER'] = LOG_TRANSFER_IN_PROGRESS
        logfile = job.logdata[0]

        try:
            create_log(job, logfile, 'tarball_PandaJob_%s_%s' % (job.jobid, job.infosys.pandaqueue), args)
        except LogFileCreationFailure as e:
            logger.warning('failed to create tar file: %s' % e)
            set_pilot_state(job=job, state="failed")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.LOGFILECREATIONFAILURE)
            return False

        if not _do_stageout(job, [logfile], ['pl', 'pw', 'w'], args.queue, title='log', output_dir=args.output_dir):
            is_success = False
            logger.warning('log transfer failed')
            job.status['LOG_TRANSFER'] = LOG_TRANSFER_FAILED
        else:
            job.status['LOG_TRANSFER'] = LOG_TRANSFER_DONE
    elif not job.logdata:
        logger.info('no log was defined - will not create log file')
        job.status['LOG_TRANSFER'] = LOG_TRANSFER_DONE

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_POST_STAGEOUT, time.time(), args)

    # generate fileinfo details to be send to Panda
    fileinfo = {}
    for e in job.outdata + job.logdata:
        if e.status in ['transferred']:
            logger.debug('got surl=%s' % e.surl)
            logger.debug('got turl=%s' % e.turl)
            fileinfo[e.lfn] = {'guid': e.guid, 'fsize': e.filesize,
                               'adler32': e.checksum.get('adler32'),
                               'surl': e.turl}

    job.fileinfo = fileinfo
    logger.info('prepared job.fileinfo=%s' % job.fileinfo)

    # WARNING THE FOLLOWING RESETS ANY PREVIOUS STAGEOUT ERRORS
    if not is_success:
        # set error code + message (a more precise error code might have been set already)
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTFAILED)
        set_pilot_state(job=job, state="failed")
        logger.warning('stage-out failed')  # with error: %d, %s (setting job state to failed)' %
        # logger.warning('stage-out failed with error: %d, %s (setting job state to failed)' %
        #  (job['pilotErrorCode'], job['pilotErrorDiag']))
        # send_state(job, args, 'failed')
        return False

    logger.info('stage-out finished correctly')

    if not job.state or (job.state and job.state == 'stageout'):  # is the job state already set? if so, don't change the state (unless it's the stageout state)
        logger.debug('changing job state from %s to finished' % job.state)
        set_pilot_state(job=job, state="finished")

    # send final server update since all transfers have finished correctly
    # send_state(job, args, 'finished', xml=dumps(fileinfodict))

    return is_success


def queue_monitoring(queues, traces, args):
    """
    Monitoring of Data queues.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while True:  # will abort when graceful_stop has been set
        time.sleep(0.5)
        if traces.pilot['command'] == 'abort':
            logger.warning('data queue monitor saw the abort instruction')
            args.graceful_stop.set()

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        # (abort at the end of the loop)
        abort = should_abort(args, label='data:queue_monitoring')

        # monitor the failed_data_in queue
        try:
            job = queues.failed_data_in.get(block=True, timeout=1)
        except queue.Empty:
            pass
        else:
            # stage-out log file then add the job to the failed_jobs queue
            job.stageout = "log"

            # TODO: put in data_out queue instead?

            if not _stage_out_new(job, args):
                logger.info("job %s failed during stage-in and stage-out of log, adding job object to failed_data_outs "
                            "queue" % job.jobid)
                #queues.failed_data_out.put(job)
                put_in_queue(job, queues.failed_data_out)
            else:
                logger.info("job %s failed during stage-in, adding job object to failed_jobs queue" % job.jobid)
                #queues.failed_jobs.put(job)
                put_in_queue(job, queues.failed_jobs)

        # monitor the finished_data_out queue
        try:
            job = queues.finished_data_out.get(block=True, timeout=1)
        except queue.Empty:
            pass
        else:
            # use the payload/transform exitCode from the job report if it exists
            if job.transexitcode == 0 and job.exitcode == 0 and job.piloterrorcodes == []:
                logger.info('finished stage-out for finished payload, adding job to finished_jobs queue')
                #queues.finished_jobs.put(job)
                put_in_queue(job, queues.finished_jobs)
            else:
                logger.info('finished stage-out (of log) for failed payload')
                #queues.failed_jobs.put(job)
                put_in_queue(job, queues.failed_jobs)

        # monitor the failed_data_out queue
        try:
            job = queues.failed_data_out.get(block=True, timeout=1)
        except queue.Empty:
            pass
        else:
            # attempt to upload the log in case the previous stage-out failure was not an SE error
            job.stageout = "log"
            set_pilot_state(job=job, state="failed")
            if not _stage_out_new(job, args):
                logger.info("job %s failed during stage-out of data file(s) as well as during stage-out of log, "
                            "adding job object to failed_jobs queue" % job.jobid)
            else:
                logger.info("job %s failed during stage-out of data file(s) - stage-out of log succeeded, adding job "
                            "object to failed_jobs queue" % job.jobid)

            #queues.failed_jobs.put(job)
            put_in_queue(job, queues.failed_jobs)

        if abort:
            break

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[data] queue_monitor thread has finished')
