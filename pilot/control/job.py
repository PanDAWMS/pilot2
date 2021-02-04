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

from __future__ import print_function  # Python 2

import os
import time
import hashlib
import random
import socket

try:
    import Queue as queue  # noqa: N813
except Exception:
    import queue  # Python 3

from json import dumps  #, loads
from re import findall

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import ExcThread, PilotException  #, JobAlreadyRunning
from pilot.info import infosys, JobData, InfoService, JobInfoProvider
from pilot.util import https
from pilot.util.auxiliary import get_batchsystem_jobid, get_job_scheduler_id, get_pilot_id, \
    set_pilot_state, get_pilot_state, check_for_final_server_update, pilot_version_banner, is_virtual_machine, \
    is_python3, show_memory_usage
from pilot.util.config import config
from pilot.util.common import should_abort, was_pilot_killed
from pilot.util.constants import PILOT_MULTIJOB_START_TIME, PILOT_PRE_GETJOB, PILOT_POST_GETJOB, PILOT_KILL_SIGNAL, LOG_TRANSFER_NOT_DONE, \
    LOG_TRANSFER_IN_PROGRESS, LOG_TRANSFER_DONE, LOG_TRANSFER_FAILED, SERVER_UPDATE_TROUBLE, SERVER_UPDATE_FINAL, \
    SERVER_UPDATE_UPDATING, SERVER_UPDATE_NOT_DONE
from pilot.util.container import execute
from pilot.util.filehandling import get_files, tail, is_json, copy, remove, write_json, establish_logging, write_file  #, read_json
from pilot.util.harvester import request_new_jobs, remove_job_request_file, parse_job_definition_file, \
    is_harvester_mode, get_worker_attributes_file, publish_job_report, publish_work_report, get_event_status_file, \
    publish_stageout_files
from pilot.util.jobmetrics import get_job_metrics
from pilot.util.math import mean
from pilot.util.monitoring import job_monitor_tasks, check_local_space
from pilot.util.monitoringtime import MonitoringTime
from pilot.util.processes import cleanup, threads_aborted
from pilot.util.proxy import get_distinguished_name
from pilot.util.queuehandling import scan_for_jobs, put_in_queue, queue_report
from pilot.util.timing import add_to_pilot_timing, timing_report, get_postgetjob_time, get_time_since, time_stamp
from pilot.util.workernode import get_disk_space, collect_workernode_info, get_node_name, get_cpu_model

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def control(queues, traces, args):
    """
    Main function of job control.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
    """

    # t = threading.current_thread()
    # logger.debug('job.control is run by thread: %s' % t.name)

    targets = {'validate': validate, 'retrieve': retrieve, 'create_data_payload': create_data_payload,
               'queue_monitor': queue_monitor, 'job_monitor': job_monitor}
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

    logger.debug('job control ending since graceful_stop has been set')
    if args.abort_job.is_set():
        if traces.pilot['command'] == 'aborting':
            logger.warning('jobs are aborting')
        elif traces.pilot['command'] == 'abort':
            logger.warning('job control detected a set abort_job (due to a kill signal)')
            traces.pilot['command'] = 'aborting'

            # find all running jobs and stop them, find all jobs in queues relevant to this module
            #abort_jobs_in_queues(queues, args.signal)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[job] control thread has finished')
    # test kill signal during end of generic workflow
    #import signal
    #os.kill(os.getpid(), signal.SIGBUS)


def _validate_job(job):
    """
    Verify job parameters for specific problems.

    :param job: job object.
    :return: Boolean.
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
    container = __import__('pilot.user.%s.container' % pilot_user, globals(), locals(), [user], 0)  # Python 2/3

    # should a container be used for the payload?
    try:
        kwargs = {'job': job}
        job.usecontainer = container.do_use_container(**kwargs)
    except Exception as e:
        logger.warning('exception caught: %s' % e)

    return True if user.verify_job(job) else False


def verify_error_code(job):
    """
    Make sure an error code is properly set.
    This makes sure that job.piloterrorcode is always set for a failed/holding job, that not only
    job.piloterrorcodes are set but not job.piloterrorcode. This function also negates the sign of the error code
    and sets job state 'holding' (instead of 'failed') if the error is found to be recoverable by a later job (user
    jobs only).

    :param job: job object.
    :return:
    """

    if job.piloterrorcode == 0 and len(job.piloterrorcodes) > 0:
        logger.warning('piloterrorcode set to first piloterrorcodes list entry: %s' % str(job.piloterrorcodes))
        job.piloterrorcode = job.piloterrorcodes[0]

    if job.piloterrorcode != 0 and job.is_analysis():
        if errors.is_recoverable(code=job.piloterrorcode):
            job.piloterrorcode = -abs(job.piloterrorcode)
            job.state = 'failed'
            logger.info('failed user job is recoverable (error code=%s)' % job.piloterrorcode)
        else:
            logger.info('failed user job is not recoverable')
    else:
        logger.info('verified error code')


def get_proper_state(job, state):
    """
    Return a proper job state to send to server.
    This function should only return 'starting', 'running', 'finished', 'holding' or 'failed'.
    If the internal job.serverstate is not yet set, it means it is the first server update, ie 'starting' should be
    sent.

    :param job: job object.
    :param state: internal pilot state (string).
    :return: valid server state (string).
    """

    logger.debug('state=%s' % state)
    logger.debug('serverstate=%s' % job.serverstate)
    if job.serverstate == "finished" or job.serverstate == "failed":
        pass
    elif job.serverstate == "" and state != "finished" and state != "failed":
        job.serverstate = 'starting'
    elif state == "finished" or state == "failed" or state == "holding":
        job.serverstate = state
    else:
        job.serverstate = 'running'
    logger.debug('serverstate=%s' % job.serverstate)

    return job.serverstate


def publish_harvester_reports(state, args, data, job, final):
    """
    Publish all reports needed by Harvester.

    :param state: job state (string).
    :param args: pilot args object.
    :param data: data structure for server update (dictionary).
    :param job: job object.
    :param final: is this the final update? (Boolean).
    :return: True if successful, False otherwise (Boolean).
    """

    # write part of the heartbeat message to worker attributes files needed by Harvester
    path = get_worker_attributes_file(args)

    # add jobStatus (state) for Harvester
    data['jobStatus'] = state

    # publish work report
    if not publish_work_report(data, path):
        logger.debug('failed to write to workerAttributesFile %s' % path)
        return False

    # check if we are in final state then write out information for output files
    if final:
        # Use the job information to write Harvester event_status.dump file
        event_status_file = get_event_status_file(args)
        if publish_stageout_files(job, event_status_file):
            logger.debug('wrote log and output files to file %s' % event_status_file)
        else:
            logger.warning('could not write log and output files to file %s' % event_status_file)
            return False

        # publish job report
        _path = os.path.join(job.workdir, config.Payload.jobreport)
        if os.path.exists(_path):
            if publish_job_report(job, args, config.Payload.jobreport):
                logger.debug('wrote job report file')
                return True
            else:
                logger.warning('failed to write job report file')
                return False
    else:
        logger.info('finished writing various report files in Harvester mode')

    return True


def write_heartbeat_to_file(data):
    """
    Write heartbeat dictionary to file.
    This is only done when server updates are not wanted.

    :param data: server data (dictionary).
    :return: True if successful, False otherwise (Boolean).
    """

    path = os.path.join(os.environ.get('PILOT_HOME'), config.Pilot.heartbeat_message)
    if write_json(path, data):
        logger.debug('heartbeat dictionary: %s' % data)
        logger.debug('wrote heartbeat to file %s' % path)
        return True
    else:
        return False


def send_state(job, args, state, xml=None, metadata=None, test_tobekilled=False):
    """
    Update the server (send heartbeat message).
    Interpret and handle any server instructions arriving with the updateJob back channel.

    :param job: job object.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :param state: job state (string).
    :param xml: optional metadata xml (string).
    :param metadata: job report metadata read as a string.
    :param test_tobekilled: emulate a tobekilled command (boolean).
    :return: boolean (True if successful, False otherwise).
    """

    state = get_proper_state(job, state)

    # should the pilot make any server updates?
    if not args.update_server:
        logger.info('pilot will not update the server (heartbeat message will be written to file)')
    tag = 'sending' if args.update_server else 'writing'

    if state == 'finished' or state == 'failed' or state == 'holding':
        final = True
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_UPDATING
        logger.info('job %s has %s - %s final server update' % (job.jobid, state, tag))

        # make sure that job.state is 'failed' if there's a set error code
        if job.piloterrorcode or job.piloterrorcodes:
            logger.warning('making sure that job.state is set to failed since a pilot error code is set')
            state = 'failed'
            job.state = state
        # make sure an error code is properly set
        elif state != 'finished':
            verify_error_code(job)
    else:
        final = False
        logger.info('job %s has state \'%s\' - %s heartbeat' % (job.jobid, state, tag))

    # build the data structure needed for getJob, updateJob
    data = get_data_structure(job, state, args, xml=xml, metadata=metadata)

    # write the heartbeat message to file if the server is not to be updated by the pilot (Nordugrid mode)
    if not args.update_server:
        logger.debug('is_harvester_mode(args) : {0}'.format(is_harvester_mode(args)))
        # if in harvester mode write to files required by harvester
        if is_harvester_mode(args):
            return publish_harvester_reports(state, args, data, job, final)
        else:
            # store the file in the main workdir
            return write_heartbeat_to_file(data)

    try:
        if config.Pilot.pandajob == 'real':
            time_before = int(time.time())
            max_attempts = 10
            attempt = 0
            done = False
            while attempt < max_attempts and not done:
                logger.info('job update attempt %d/%d' % (attempt + 1, max_attempts))

                # get the URL for the PanDA server from pilot options or from config
                pandaserver = get_panda_server(args.url, args.port)

                res = https.request('{pandaserver}/server/panda/updateJob'.format(pandaserver=pandaserver), data=data)
                if res is not None:
                    done = True
                attempt += 1

            time_after = int(time.time())
            logger.info('server updateJob request completed in %ds for job %s' % (time_after - time_before, job.jobid))
            logger.info("server responded with: res = %s" % str(res))

            show_memory_usage()

            if res is not None:
                # does the server update contain any backchannel information? if so, update the job object
                handle_backchannel_command(res, job, args, test_tobekilled=test_tobekilled)

                if final:
                    os.environ['SERVER_UPDATE'] = SERVER_UPDATE_FINAL
                    logger.debug('set SERVER_UPDATE=SERVER_UPDATE_FINAL')
                return True
        else:
            logger.info('skipping job update for fake test job')
            return True

    except Exception as e:
        logger.warning('exception caught while sending https request: %s' % e)
        logger.warning('possibly offending data: %s' % data)
        pass

    if final:
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_TROUBLE
        logger.debug('set SERVER_UPDATE=SERVER_UPDATE_TROUBLE')

    return False


def get_job_status_from_server(job_id, url, port):
    """
    Return the current status of job <jobId> from the dispatcher.
    typical dispatcher response: 'status=finished&StatusCode=0'
    StatusCode  0: succeeded
               10: time-out
               20: general error
               30: failed
    In the case of time-out, the dispatcher will be asked one more time after 10 s.

    :param job_id: PanDA job id (int).
    :param url: PanDA server URL (string).
    :param port: PanDA server port (int).
    :return: status (string; e.g. holding), attempt_nr (int), status_code (int)
    """

    status = 'unknown'
    attempt_nr = 0
    status_code = 0
    if config.Pilot.pandajob == 'fake':
        return status, attempt_nr, status_code

    data = {}
    data['ids'] = job_id

    # get the URL for the PanDA server from pilot options or from config
    pandaserver = get_panda_server(url, port)

    # ask dispatcher about lost job status
    trial = 1
    max_trials = 2

    while trial <= max_trials:
        try:
            # open connection
            ret = https.request('{pandaserver}/server/panda/getStatus'.format(pandaserver=pandaserver), data=data)
            response = ret[1]
            logger.info("response: %s" % str(response))
            if response:
                try:
                    # decode the response
                    # eg. var = ['status=notfound', 'attemptNr=0', 'StatusCode=0']
                    # = response

                    status = response['status']  # e.g. 'holding'
                    attempt_nr = int(response['attemptNr'])  # e.g. '0'
                    status_code = int(response['StatusCode'])  # e.g. '0'
                except Exception as e:
                    logger.warning(
                        "exception: dispatcher did not return allowed values: %s, %s" % (str(ret), e))
                    status = "unknown"
                    attempt_nr = -1
                    status_code = 20
                else:
                    logger.debug('server job status=%s, attempt_nr=%d, status_code=%d' % (status, attempt_nr, status_code))
            else:
                logger.warning("dispatcher did not return allowed values: %s" % str(ret))
                status = "unknown"
                attempt_nr = -1
                status_code = 20
        except Exception as e:
            logger.warning("could not interpret job status from dispatcher: %s" % e)
            status = 'unknown'
            attempt_nr = -1
            status_code = -1
            break
        else:
            if status_code == 0:  # success
                break
            elif status_code == 10:  # time-out
                trial += 1
                time.sleep(10)
                continue
            elif status_code == 20:  # other error
                if ret[0] == 13056 or ret[0] == '13056':
                    logger.warning("wrong certificate used with curl operation? (encountered error 13056)")
                break
            else:  # general error
                break

    return status, attempt_nr, status_code


def get_panda_server(url, port):
    """
    Get the URL for the PanDA server.

    :param url: URL string, if set in pilot option (port not included).
    :param port: port number, if set in pilot option (int).
    :return: full URL (either from pilot options or from config file)
    """

    if url != '' and port != 0:
        pandaserver = '%s:%s' % (url, port)
    else:
        pandaserver = config.Pilot.pandaserver

    # add randomization for PanDA server
    default = 'pandaserver.cern.ch'
    if default in pandaserver:
        rnd = random.choice([socket.getfqdn(vv) for vv in set([v[-1][0] for v in socket.getaddrinfo(default, 25443, socket.AF_INET)])])
        pandaserver = pandaserver.replace(default, rnd)
        logger.debug('updated %s to %s' % (default, pandaserver))

    return pandaserver


def handle_backchannel_command(res, job, args, test_tobekilled=False):
    """
    Does the server update contain any backchannel information? if so, update the job object.

    :param res: server response (dictionary).
    :param job: job object.
    :param args: pilot args object.
    :param test_tobekilled: emulate a tobekilled command (boolean).
    :return:
    """

    if test_tobekilled:
        logger.info('faking a \'tobekilled\' command')
        res['command'] = 'tobekilled'

    if 'command' in res and res.get('command') != 'NULL':
        # look for 'tobekilled', 'softkill', 'debug', 'debugoff'
        if res.get('command') == 'tobekilled':
            logger.info('pilot received a panda server signal to kill job %s at %s' %
                        (job.jobid, time_stamp()))
            set_pilot_state(job=job, state="failed")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PANDAKILL)
            args.abort_job.set()
        elif res.get('command') == 'softkill':
            logger.info('pilot received a panda server signal to softkill job %s at %s' %
                        (job.jobid, time_stamp()))
            # event service kill instruction
        elif res.get('command') == 'debug':
            logger.info('pilot received a command to turn on debug mode from the server')
            job.debug = True
        elif res.get('command') == 'debugoff':
            logger.info('pilot received a command to turn off debug mode from the server')
            job.debug = False
        else:
            logger.warning('received unknown server command via backchannel: %s' % res.get('command'))


def get_data_structure(job, state, args, xml=None, metadata=None):
    """
    Build the data structure needed for getJob, updateJob.

    :param job: job object.
    :param state: state of the job (string).
    :param args:
    :param xml: optional XML string.
    :param metadata: job report metadata read as a string.
    :return: data structure (dictionary).
    """

    logger.debug('building data structure to be sent to server with heartbeat')

    data = {'jobId': job.jobid,
            'state': state,
            'timestamp': time_stamp(),
            'siteName': os.environ.get('PILOT_SITENAME'),  # args.site,
            'node': get_node_name()}

    data['attemptNr'] = job.attemptnr

    schedulerid = get_job_scheduler_id()
    if schedulerid:
        data['schedulerID'] = schedulerid

    pilotid = get_pilot_id()
    if pilotid:
        pilotversion = os.environ.get('PILOT_VERSION')

        # report the batch system job id, if available
        batchsystem_type, batchsystem_id = get_batchsystem_jobid()

        if batchsystem_type:
            data['pilotID'] = "%s|%s|%s|%s" % \
                              (pilotid, batchsystem_type, args.version_tag, pilotversion)
            data['batchID'] = batchsystem_id
        else:
            data['pilotID'] = "%s|%s|%s" % (pilotid, args.version_tag, pilotversion)

    starttime = get_postgetjob_time(job.jobid, args)
    if starttime:
        data['startTime'] = starttime

    job_metrics = get_job_metrics(job)
    if job_metrics:
        data['jobMetrics'] = job_metrics

    if xml is not None:
        data['xml'] = xml
    if metadata is not None:
        data['metaData'] = metadata

    # in debug mode, also send a tail of the latest log file touched by the payload
    if job.debug:
        stdout_tail = get_payload_log_tail(job)
        if stdout_tail:
            data['stdout'] = stdout_tail

    # add the core count
    if job.corecount and job.corecount != 'null' and job.corecount != 'NULL':
        data['coreCount'] = job.corecount
        #data['coreCount'] = mean(job.corecounts) if job.corecounts else job.corecount
    if job.corecounts:
        _mean = mean(job.corecounts)
        logger.info('mean actualcorecount: %f' % _mean)
        data['meanCoreCount'] = _mean

    # get the number of events, should report in heartbeat in case of preempted.
    if job.nevents != 0:
        data['nEvents'] = job.nevents
        logger.info("total number of processed events: %d (read)" % job.nevents)
    else:
        logger.info("payload/TRF did not report the number of read events")

    # get the CU consumption time
    constime = get_cpu_consumption_time(job.cpuconsumptiontime)
    if constime and constime != -1:
        data['cpuConsumptionTime'] = constime
        data['cpuConsumptionUnit'] = job.cpuconsumptionunit + "+" + get_cpu_model()
        data['cpuConversionFactor'] = job.cpuconversionfactor

    # add memory information if available
    add_memory_info(data, job.workdir, name=job.memorymonitor)
    if state == 'finished' or state == 'failed':
        add_timing_and_extracts(data, job, state, args)
        add_error_codes(data, job)

    return data


def add_error_codes(data, job):
    """
    Add error codes to data structure.

    :param data: data dictionary.
    :param job: job object.
    :return:
    """

    # error codes
    pilot_error_code = job.piloterrorcode
    pilot_error_codes = job.piloterrorcodes
    if pilot_error_codes != []:
        logger.warning('pilotErrorCodes = %s (will report primary/first error code)' % str(pilot_error_codes))
        data['pilotErrorCode'] = pilot_error_codes[0]
    else:
        data['pilotErrorCode'] = pilot_error_code

    # add error info
    pilot_error_diag = job.piloterrordiag
    pilot_error_diags = job.piloterrordiags
    if pilot_error_diags != []:
        logger.warning('pilotErrorDiags = %s (will report primary/first error diag)' % str(pilot_error_diags))
        data['pilotErrorDiag'] = pilot_error_diags[0]
    else:
        data['pilotErrorDiag'] = pilot_error_diag
    data['transExitCode'] = job.transexitcode
    data['exeErrorCode'] = job.exeerrorcode
    data['exeErrorDiag'] = job.exeerrordiag


def get_cpu_consumption_time(cpuconsumptiontime):
    """
    Get the CPU consumption time.
    The function makes sure that the value exists and is within allowed limits (< 10^9).

    :param cpuconsumptiontime: CPU consumption time (int/None).
    :return: properly set CPU consumption time (int/None).
    """

    constime = None

    try:
        constime = int(cpuconsumptiontime)
    except Exception:
        constime = None
    if constime and constime > 10 ** 9:
        logger.warning("unrealistic cpuconsumptiontime: %d (reset to -1)" % constime)
        constime = -1

    return constime


def add_timing_and_extracts(data, job, state, args):
    """
    Add timing info and log extracts to data structure for a completed job (finished or failed) to be sent to server.
    Note: this function updates the data dictionary.

    :param data: data structure (dictionary).
    :param job: job object.
    :param state: state of the job (string).
    :param args: pilot args.
    :return:
    """

    time_getjob, time_stagein, time_payload, time_stageout, time_total_setup = timing_report(job.jobid, args)
    data['pilotTiming'] = "%s|%s|%s|%s|%s" % \
                          (time_getjob, time_stagein, time_payload, time_stageout, time_total_setup)

    # add log extracts (for failed/holding jobs or for jobs with outbound connections)
    extracts = ""
    if state == 'failed' or state == 'holding':
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.diagnose' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
        extracts = user.get_log_extracts(job, state)
        if extracts != "":
            logger.warning('\nXXXXXXXXXXXXXXXXXXXXX[begin log extracts]\n%s\nXXXXXXXXXXXXXXXXXXXXX[end log extracts]' % extracts)
    data['pilotLog'] = extracts[:1024]
    data['endTime'] = time.time()


def add_memory_info(data, workdir, name=""):
    """
    Add memory information (if available) to the data structure that will be sent to the server with job updates
    Note: this function updates the data dictionary.

    :param data: data structure (dictionary).
    :param workdir: working directory of the job (string).
    :param name: name of memory monitor (string).
    :return:
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    utilities = __import__('pilot.user.%s.utilities' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
    try:
        #for key in job.utilities
        utility_node = utilities.get_memory_monitor_info(workdir, name=name)
        data.update(utility_node)
    except Exception as e:
        logger.info('memory information not available: %s' % e)
        pass


def get_list_of_log_files():
    """
    Return a list of log files produced by the payload.

    :return: list of log files.
    """

    list_of_files = get_files()
    if not list_of_files:  # some TRFs produce logs with different naming scheme
        list_of_files = get_files(pattern="log.*")

    return list_of_files


def get_payload_log_tail(job):
    """
    Return the tail of the payload stdout or its latest updated log file.

    :param job: job object.
    :return: tail of stdout (string).
    """

    stdout_tail = ""

    # find the latest updated log file
    list_of_files = get_list_of_log_files()
    if not list_of_files:
        logger.info('no log files were found (will use default %s)' % config.Payload.payloadstdout)
        list_of_files = [os.path.join(job.workdir, config.Payload.payloadstdout)]  # get_files(pattern=config.Payload.payloadstdout)

    try:
        latest_file = max(list_of_files, key=os.path.getmtime)
        logger.info('tail of file %s will be added to heartbeat' % latest_file)

        # now get the tail of the found log file and protect against potentially large tails
        stdout_tail = latest_file + "\n" + tail(latest_file)
        stdout_tail = stdout_tail[-2048:]
    except Exception as e:
        logger.warning('failed to get payload stdout tail: %s' % e)

    return stdout_tail


def validate(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.jobs.get(block=True, timeout=1)
        except queue.Empty:
            continue

        traces.pilot['nr_jobs'] += 1

        # set the environmental variable for the task id
        os.environ['PanDA_TaskID'] = str(job.taskid)
        logger.info('processing PanDA job %s from task %s' % (job.jobid, job.taskid))

        if _validate_job(job):

            # Define a new parent group
            os.setpgrp()

            job_dir = os.path.join(args.mainworkdir, 'PanDA_Pilot-%s' % job.jobid)
            logger.debug('creating job working directory: %s' % job_dir)
            try:
                os.mkdir(job_dir)
                os.chmod(job_dir, 0o770)
                job.workdir = job_dir
            except Exception as e:
                logger.debug('cannot create working directory: %s' % str(e))
                traces.pilot['error_code'] = errors.MKDIR
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(traces.pilot['error_code'])
                job.piloterrordiag = e
                put_in_queue(job, queues.failed_jobs)
                break

#            try:
#                # stream the job object to file
#                job_dict = job.to_json()
#                write_json(os.path.join(job.workdir, 'job.json'), job_dict)
#            except Exception as e:
#                logger.debug('exception caught: %s' % e)
#            else:
#                try:
#                    _job_dict = read_json(os.path.join(job.workdir, 'job.json'))
#                    job_dict = loads(_job_dict)
#                    _job = JobData(job_dict, use_kmap=False)
#                except Exception as e:
#                    logger.warning('exception caught: %s' % e)
            logger.debug('symlinking pilot log')
            try:
                os.symlink('../%s' % config.Pilot.pilotlog, os.path.join(job_dir, config.Pilot.pilotlog))
            except Exception as e:
                logger.warning('cannot symlink pilot log: %s' % str(e))

            # pre-cleanup
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            utilities = __import__('pilot.user.%s.utilities' % pilot_user, globals(), locals(), [pilot_user],
                                   0)  # Python 2/3
            try:
                utilities.precleanup()
            except Exception as e:
                logger.warning('exception caught: %s' % e)

            # store the PanDA job id for the wrapper to pick up
            store_jobid(job.jobid, args.sourcedir)

            put_in_queue(job, queues.validated_jobs)

        else:
            logger.debug('Failed to validate job=%s' % job.jobid)
            put_in_queue(job, queues.failed_jobs)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[job] validate thread has finished')


def store_jobid(jobid, init_dir):
    """
    Store the PanDA job id in a file that can be picked up by the wrapper for other reporting.

    :param jobid: job id (int).
    :param init_dir: pilot init dir (string).
    :return:
    """

    try:
        path = os.path.join(os.path.join(init_dir, 'pilot2'), config.Pilot.jobid_file)
        path = path.replace('pilot2/pilot2', 'pilot2')  # dirty fix for bad paths
        mode = 'a' if os.path.exists(path) else 'w'
        logger.debug('path=%s  mode=%s' % (path, mode))
        write_file(path, "%s\n" % str(jobid), mode=mode, mute=False)
    except Exception as e:
        logger.warning('exception caught while trying to store job id: %s' % e)


def create_data_payload(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.validated_jobs.get(block=True, timeout=1)
        except queue.Empty:
            continue

        if job.indata:
            # if the job has input data, put the job object in the data_in queue which will trigger stage-in
            set_pilot_state(job=job, state='stagein')
            show_memory_usage()
            put_in_queue(job, queues.data_in)

        else:
            # if the job does not have any input data, then pretend that stage-in has finished and put the job
            # in the finished_data_in queue
            put_in_queue(job, queues.finished_data_in)

        put_in_queue(job, queues.payloads)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[job] create_data_payload thread has finished')


def get_task_id():
    """
    Return the task id for the current job.
    Note: currently the implementation uses an environmental variable to store this number (PanDA_TaskID).

    :return: task id (string). Returns empty string in case of error.
    """

    if "PanDA_TaskID" in os.environ:
        taskid = os.environ["PanDA_TaskID"]
    else:
        logger.warning('PanDA_TaskID not set in environment')
        taskid = ""

    return taskid


def get_job_label(args):
    """
    Return a proper job label.
    The function returns a job label that corresponds to the actual pilot version, ie if the pilot is a development
    version (ptest or rc_test2) or production version (managed or user).
    Example: -i RC -> job_label = rc_test2.
    NOTE: it should be enough to only use the job label, -j rc_test2 (and not specify -i RC at all).

    :param args: pilot args object.
    :return: job_label (string).
    """

    # PQ status
    status = infosys.queuedata.status

    if args.version_tag == 'RC' and args.job_label == 'rc_test2':
        job_label = 'rc_test2'
    elif args.version_tag == 'RC' and args.job_label == 'ptest':
        job_label = args.job_label
    elif args.version_tag == 'RCM' and args.job_label == 'ptest':
        job_label = 'rcm_test2'
    elif args.version_tag == 'ALRB':
        job_label = 'rc_alrb'
    elif status == 'test' and args.job_label != 'ptest':
        logger.warning('PQ status set to test - will use job label / prodSourceLabel test')
        job_label = 'test'
    else:
        job_label = args.job_label

    return job_label


def get_dispatcher_dictionary(args):
    """
    Return a dictionary with required fields for the dispatcher getJob operation.

    The dictionary should contain the following fields: siteName, computingElement (queue name),
    prodSourceLabel (e.g. user, test, ptest), diskSpace (available disk space for a job in MB),
    workingGroup, countryGroup, cpu (float), mem (float) and node (worker node name).

    workingGroup, countryGroup and allowOtherCountry
    we add a new pilot setting allowOtherCountry=True to be used in conjunction with countryGroup=us for
    US pilots. With these settings, the Panda server will produce the desired behaviour of dedicated X% of
    the resource exclusively (so long as jobs are available) to countryGroup=us jobs. When allowOtherCountry=false
    this maintains the behavior relied on by current users of the countryGroup mechanism -- to NOT allow
    the resource to be used outside the privileged group under any circumstances.

    :param args: arguments (e.g. containing queue name, queuedata dictionary, etc).
    :returns: dictionary prepared for the dispatcher getJob operation.
    """

    _diskspace = get_disk_space(infosys.queuedata)

    _mem, _cpu, _disk = collect_workernode_info()

    _nodename = get_node_name()

    # override for RC dev pilots
    job_label = get_job_label(args)

    data = {
        'siteName': infosys.queuedata.resource,  # next: remove redundant '-r' option of pilot.py
        'computingElement': args.queue,
        'prodSourceLabel': job_label,
        'diskSpace': _diskspace,
        'workingGroup': args.working_group,
        'cpu': _cpu,
        'mem': _mem,
        'node': _nodename
    }

    if args.jobtype != "":
        data['jobType'] = args.jobtype

    if args.allow_other_country != "":
        data['allowOtherCountry'] = args.allow_other_country

    if args.country_group != "":
        data['countryGroup'] = args.country_group

    if args.job_label == 'self':
        dn = get_distinguished_name()
        data['prodUserID'] = dn

    taskid = get_task_id()
    if taskid != "" and args.allow_same_user:
        data['taskID'] = taskid
        logger.info("will download a new job belonging to task id: %s" % (data['taskID']))

    if args.resource_type != "":
        data['resourceType'] = args.resource_type

    # add harvester fields
    if 'HARVESTER_ID' in os.environ:
        data['harvester_id'] = os.environ.get('HARVESTER_ID')
    if 'HARVESTER_WORKER_ID' in os.environ:
        data['worker_id'] = os.environ.get('HARVESTER_WORKER_ID')

    return data


def proceed_with_getjob(timefloor, starttime, jobnumber, getjob_requests, harvester, verify_proxy, traces):
    """
    Can we proceed with getjob?
    We may not proceed if we have run out of time (timefloor limit), if the proxy is too short, if disk space is too
    small or if we have already proceed enough jobs.

    :param timefloor: timefloor limit (s)
    :param starttime: start time of retrieve() (s)
    :param jobnumber: number of downloaded jobs
    :param getjob_requests: number of getjob requests
    :param harvester: True if Harvester is used, False otherwise. Affects the max number of getjob reads (from file).
    :param verify_proxy: True if the proxy should be verified. False otherwise.
    :param traces: traces object (to be able to propagate a proxy error all the way back to the wrapper).
    :return: Boolean.
    """

    # use for testing thread exceptions. the exception will be picked up by ExcThread run() and caught in job.control()
    # raise NoLocalSpace('testing exception from proceed_with_getjob')

    #timefloor = 600
    currenttime = time.time()

    # should the proxy be verified?
    if verify_proxy:
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        userproxy = __import__('pilot.user.%s.proxy' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # is the proxy still valid?
        exit_code, diagnostics = userproxy.verify_proxy()
        if traces.pilot['error_code'] == 0:  # careful so we don't overwrite another error code
            traces.pilot['error_code'] = exit_code
        if exit_code == errors.NOPROXY or exit_code == errors.NOVOMSPROXY:
            logger.warning(diagnostics)
            return False

    # is there enough local space to run a job?
    ec, diagnostics = check_local_space()
    if ec != 0:
        traces.pilot['error_code'] = errors.NOLOCALSPACE
        return False

    maximum_getjob_requests = 60 if harvester else config.Pilot.maximum_getjob_requests  # 1 s apart (if harvester)
    if getjob_requests > int(maximum_getjob_requests):
        logger.warning('reached maximum number of getjob requests (%s) -- will abort pilot' %
                       config.Pilot.maximum_getjob_requests)
        # use singleton:
        # instruct the pilot to wrap up quickly
        os.environ['PILOT_WRAP_UP'] = 'QUICKLY'
        return False

    if timefloor == 0 and jobnumber > 0:
        logger.warning("since timefloor is set to 0, pilot was only allowed to run one job")
        # use singleton:
        # instruct the pilot to wrap up quickly
        os.environ['PILOT_WRAP_UP'] = 'QUICKLY'
        return False

    if (currenttime - starttime > timefloor) and jobnumber > 0:
        logger.warning("the pilot has run out of time (timefloor=%d has been passed)" % timefloor)
        # use singleton:
        # instruct the pilot to wrap up quickly
        os.environ['PILOT_WRAP_UP'] = 'QUICKLY'
        return False

    # timefloor not relevant for the first job
    if jobnumber > 0:
        logger.info('since timefloor=%d s and only %d s has passed since launch, pilot can run another job' %
                    (timefloor, currenttime - starttime))

    if harvester and jobnumber > 0:
        # unless it's the first job (which is preplaced in the init dir), instruct Harvester to place another job
        # in the init dir
        logger.info('asking Harvester for another job')
        request_new_jobs()

    if os.environ.get('SERVER_UPDATE', '') == SERVER_UPDATE_UPDATING:
        logger.info('still updating previous job, will not ask for a new job yet')
        return False

    os.environ['SERVER_UPDATE'] = SERVER_UPDATE_NOT_DONE
    return True


def getjob_server_command(url, port):
    """
    Prepare the getJob server command.

    :param url: PanDA server URL (string)
    :param port: PanDA server port
    :return: full server command (URL string)
    """

    if url != "":
        port_pattern = '.:([0-9]+)'
        if not findall(port_pattern, url):
            url = url + ':%s' % port
        else:
            logger.debug('URL already contains port: %s' % url)
    else:
        url = config.Pilot.pandaserver
    if url == "":
        logger.fatal('PanDA server url not set (either as pilot option or in config file)')
    elif not url.startswith("http"):
        url = 'https://' + url
        logger.warning('detected missing protocol in server url (added)')

    return '{pandaserver}/server/panda/getJob'.format(pandaserver=url)


def get_job_definition_from_file(path, harvester):
    """
    Get a job definition from a pre-placed file.
    In Harvester mode, also remove any existing job request files since it is no longer needed/wanted.

    :param path: path to job definition file.
    :param harvester: True if Harvester is being used (determined from args.harvester), otherwise False
    :return: job definition dictionary.
    """

    # remove any existing Harvester job request files (silent in non-Harvester mode) and read the JSON
    if harvester:
        remove_job_request_file()
        if is_json(path):
            job_definition_list = parse_job_definition_file(path)
            if not job_definition_list:
                logger.warning('no jobs were found in Harvester job definitions file: %s' % path)
                return {}
            else:
                # remove the job definition file from the original location, place a renamed copy in the pilot dir
                new_path = os.path.join(os.environ.get('PILOT_HOME'), 'job_definition.json')
                copy(path, new_path)
                remove(path)

                # note: the pilot can only handle one job at the time from Harvester
                return job_definition_list[0]

    # old style
    res = {}
    with open(path, 'r') as jobdatafile:
        response = jobdatafile.read()
        if len(response) == 0:
            logger.fatal('encountered empty job definition file: %s' % path)
            res = None  # this is a fatal error, no point in continuing as the file will not be replaced
        else:
            # parse response message
            # logger.debug('%s:\n\n%s\n\n' % (path, response))
            try:
                from urlparse import parse_qsl  # Python 2
            except Exception:
                from urllib.parse import parse_qsl  # Python 3
            datalist = parse_qsl(response, keep_blank_values=True)

            # convert to dictionary
            for d in datalist:
                res[d[0]] = d[1]

    if os.path.exists(path):
        remove(path)

    return res


def get_job_definition_from_server(args):
    """
    Get a job definition from a server.

    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return: job definition dictionary.
    """

    res = {}

    # get the job dispatcher dictionary
    data = get_dispatcher_dictionary(args)

    cmd = getjob_server_command(args.url, args.port)
    if cmd != "":
        logger.info('executing server command: %s' % cmd)
        res = https.request(cmd, data=data)

    return res


def locate_job_definition(args):
    """
    Locate the job definition file among standard locations.

    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return: path (string).
    """

    paths = [os.path.join("%s/.." % args.sourcedir, config.Pilot.pandajobdata),
             os.path.join(args.sourcedir, config.Pilot.pandajobdata),
             os.path.join(os.environ['PILOT_WORK_DIR'], config.Pilot.pandajobdata)]

    if args.harvester_workdir:
        paths.append(os.path.join(args.harvester_workdir, config.Harvester.pandajob_file))
    if 'HARVESTER_WORKDIR' in os.environ:
        paths.append(os.path.join(os.environ['HARVESTER_WORKDIR'], config.Harvester.pandajob_file))

    path = ""
    for _path in paths:
        if os.path.exists(_path):
            path = _path
            break

    if path == "":
        logger.info('did not find any local job definition file')

    return path


def get_job_definition(args):
    """
    Get a job definition from a source (server or pre-placed local file).

    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return: job definition dictionary.
    """

    res = {}
    path = locate_job_definition(args)

    # should we run a normal 'real' job or a 'fake' job?
    if config.Pilot.pandajob == 'fake':
        logger.info('will use a fake PanDA job')
        res = get_fake_job()
    elif os.path.exists(path):
        logger.info('will read job definition from file %s' % path)
        res = get_job_definition_from_file(path, args.harvester)
    else:
        if args.harvester and args.harvester_submitmode.lower() == 'push':
            pass  # local job definition file not found (go to sleep)
        else:
            logger.info('will download job definition from server')
            res = get_job_definition_from_server(args)

    return res


def get_fake_job(input=True):
    """
    Return a job definition for internal pilot testing.
    Note: this function is only used for testing purposes. The job definitions below are ATLAS specific.

    :param input: Boolean, set to False if no input files are wanted
    :return: job definition (dictionary).
    """

    res = None

    # create hashes
    hash = hashlib.md5()
    hash.update(str(time.time()))
    log_guid = hash.hexdigest()
    hash.update(str(time.time()))
    guid = hash.hexdigest()
    hash.update(str(time.time()))
    job_name = hash.hexdigest()

    if config.Pilot.testjobtype == 'production':
        logger.info('creating fake test production job definition')
        res = {'jobsetID': 'NULL',
               'logGUID': log_guid,
               'cmtConfig': 'x86_64-slc6-gcc48-opt',
               'prodDBlocks': 'user.mlassnig:user.mlassnig.pilot.test.single.hits',
               'dispatchDBlockTokenForOut': 'NULL,NULL',
               'destinationDBlockToken': 'NULL,NULL',
               'destinationSE': 'AGLT2_TEST',
               'realDatasets': job_name,
               'prodUserID': 'no_one',
               'GUID': guid,
               'realDatasetsIn': 'user.mlassnig:user.mlassnig.pilot.test.single.hits',
               'nSent': 0,
               'cloud': 'US',
               'StatusCode': 0,
               'homepackage': 'AtlasProduction/20.1.4.14',
               'inFiles': 'HITS.06828093._000096.pool.root.1',
               'processingType': 'pilot-ptest',
               'ddmEndPointOut': 'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
               'fsize': '94834717',
               'fileDestinationSE': 'AGLT2_TEST,AGLT2_TEST',
               'scopeOut': 'panda',
               'minRamCount': 0,
               'jobDefinitionID': 7932,
               'maxWalltime': 'NULL',
               'scopeLog': 'panda',
               'transformation': 'Reco_tf.py',
               'maxDiskCount': 0,
               'coreCount': 1,
               'prodDBlockToken': 'NULL',
               'transferType': 'NULL',
               'destinationDblock': job_name,
               'dispatchDBlockToken': 'NULL',
               'jobPars': '--maxEvents=1 --inputHITSFile HITS.06828093._000096.pool.root.1 --outputRDOFile RDO_%s.root' % job_name,
               'attemptNr': 0,
               'swRelease': 'Atlas-20.1.4',
               'nucleus': 'NULL',
               'maxCpuCount': 0,
               'outFiles': 'RDO_%s.root,%s.job.log.tgz' % (job_name, job_name),
               'currentPriority': 1000,
               'scopeIn': 'mc15_13TeV',
               'PandaID': '0',
               'sourceSite': 'NULL',
               'dispatchDblock': 'NULL',
               'prodSourceLabel': 'ptest',
               'checksum': 'ad:5d000974',
               'jobName': job_name,
               'ddmEndPointIn': 'UTA_SWT2_DATADISK',
               'taskID': 'NULL',
               'logFile': '%s.job.log.tgz' % job_name}
    elif config.Pilot.testjobtype == 'user':
        logger.info('creating fake test user job definition')
        res = {'jobsetID': 'NULL',
               'logGUID': log_guid,
               'cmtConfig': 'x86_64-slc6-gcc49-opt',
               'prodDBlocks': 'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               'dispatchDBlockTokenForOut': 'NULL,NULL',
               'destinationDBlockToken': 'NULL,NULL',
               'destinationSE': 'ANALY_SWT2_CPB',
               'realDatasets': job_name,
               'prodUserID': 'None',
               'GUID': guid,
               'realDatasetsIn': 'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               'nSent': '0',
               'cloud': 'US',
               'StatusCode': 0,
               'homepackage': 'AnalysisTransforms-AtlasDerivation_20.7.6.4',
               'inFiles': 'AOD.07709524._000050.pool.root.1',
               'processingType': 'pilot-ptest',
               'ddmEndPointOut': 'SWT2_CPB_SCRATCHDISK,SWT2_CPB_SCRATCHDISK',
               'fsize': '1564780952',
               'fileDestinationSE': 'ANALY_SWT2_CPB,ANALY_SWT2_CPB',
               'scopeOut': 'user.gangarbt',
               'minRamCount': '0',
               'jobDefinitionID': '9445',
               'maxWalltime': 'NULL',
               'scopeLog': 'user.gangarbt',
               'transformation': 'http://pandaserver.cern.ch:25080/trf/user/runAthena-00-00-11',
               'maxDiskCount': '0',
               'coreCount': '1',
               'prodDBlockToken': 'NULL',
               'transferType': 'NULL',
               'destinationDblock': job_name,
               'dispatchDBlockToken': 'NULL',
               'jobPars': '-a sources.20115461.derivation.tgz -r ./ -j "Reco_tf.py '
                           '--inputAODFile AOD.07709524._000050.pool.root.1 --outputDAODFile test.pool.root '
                           '--reductionConf HIGG3D1" -i "[\'AOD.07709524._000050.pool.root.1\']" -m "[]" -n "[]" --trf'
                           ' --useLocalIO --accessmode=copy -o '
                           '"{\'IROOT\': [(\'DAOD_HIGG3D1.test.pool.root\', \'%s.root\')]}" '
                           '--sourceURL https://aipanda012.cern.ch:25443' % (job_name),
               'attemptNr': '0',
               'swRelease': 'Atlas-20.7.6',
               'nucleus': 'NULL',
               'maxCpuCount': '0',
               'outFiles': '%s.root,%s.job.log.tgz' % (job_name, job_name),
               'currentPriority': '1000',
               'scopeIn': 'data15_13TeV',
               'PandaID': '0',
               'sourceSite': 'NULL',
               'dispatchDblock': 'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               'prodSourceLabel': 'ptest',
               'checksum': 'ad:b11f45a7',
               'jobName': job_name,
               'ddmEndPointIn': 'SWT2_CPB_SCRATCHDISK',
               'taskID': 'NULL',
               'logFile': '%s.job.log.tgz' % job_name}
    else:
        logger.warning('unknown test job type: %s' % config.Pilot.testjobtype)

    if res:
        if not input:
            res['inFiles'] = 'NULL'
            res['GUID'] = 'NULL'
            res['scopeIn'] = 'NULL'
            res['fsize'] = 'NULL'
            res['realDatasetsIn'] = 'NULL'
            res['checksum'] = 'NULL'

        if config.Pilot.testtransfertype == "NULL" or config.Pilot.testtransfertype == 'direct':
            res['transferType'] = config.Pilot.testtransfertype
        else:
            logger.warning('unknown test transfer type: %s (ignored)' % config.Pilot.testtransfertype)

        if config.Pilot.testjobcommand == 'sleep':
            res['transformation'] = 'sleep'
            res['jobPars'] = '1'
            res['inFiles'] = ''
            res['outFiles'] = ''

        # convert to unicode for Python 2
        try:  # in case some later version of Python 3 has problems using u'' (seems ok with 3.7 at least)
            if not is_python3():
                _res = {}
                for entry in res:
                    if type(res[entry]) is str:
                        _res[u'%s' % entry] = u'%s' % res[entry]
                    else:
                        _res[u'%s' % entry] = res[entry]
                res = _res
        except Exception:
            pass
    return res


def get_job_retrieval_delay(harvester):
    """
    Return the proper delay between job retrieval attempts.
    In Harvester mode, the pilot will look once per second for a job definition file.

    :param harvester: True if Harvester is being used (determined from args.harvester), otherwise False
    :return: sleep (s)
    """

    return 1 if harvester else 60


def retrieve(queues, traces, args):  # noqa: C901
    """
    Retrieve all jobs from a source.

    The job definition is a json dictionary that is either present in the launch
    directory (preplaced) or downloaded from a server specified by `args.url`.

    The function retrieves the job definition from the proper source and places
    it in the `queues.jobs` queue.

    WARNING: this function is nearly too complex. Be careful with adding more lines as flake8 will fail it.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :raises PilotException: if create_job fails (e.g. because queuedata could not be downloaded).
    :return:
    """

    timefloor = infosys.queuedata.timefloor
    starttime = time.time()

    jobnumber = 0  # number of downloaded jobs
    getjob_requests = 0  # number of getjob requests

    print_node_info()

    while not args.graceful_stop.is_set():

        time.sleep(0.5)
        getjob_requests += 1

        if not proceed_with_getjob(timefloor, starttime, jobnumber, getjob_requests, args.harvester, args.verify_proxy, traces):
            # do not set graceful stop if pilot has not finished sending the final job update
            # i.e. wait until SERVER_UPDATE is DONE_FINAL
            check_for_final_server_update(args.update_server)
            args.graceful_stop.set()
            break

        # store time stamp
        time_pre_getjob = time.time()

        # get a job definition from a source (file or server)
        res = get_job_definition(args)
        logger.info('job definition = %s' % str(res))

        if res is None:
            logger.fatal('fatal error in job download loop - cannot continue')
            # do not set graceful stop if pilot has not finished sending the final job update
            # i.e. wait until SERVER_UPDATE is DONE_FINAL
            check_for_final_server_update(args.update_server)
            args.graceful_stop.set()
            break

        if not res:
            delay = get_job_retrieval_delay(args.harvester)
            if not args.harvester:
                logger.warning('did not get a job -- sleep %d s and repeat' % delay)
            for i in range(delay):
                if args.graceful_stop.is_set():
                    break
                time.sleep(1)
        else:
            # it seems the PanDA server returns StatusCode as an int, but the aCT returns it as a string
            # note: StatusCode keyword is not available in job definition files from Harvester (not needed)
            if 'StatusCode' in res and res['StatusCode'] != '0' and res['StatusCode'] != 0:
                logger.warning('did not get a job -- sleep 60s and repeat -- status: %s' % res['StatusCode'])
                for i in range(60):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(1)
            else:
                # create the job object out of the raw dispatcher job dictionary
                try:
                    show_memory_usage()
                    job = create_job(res, args.queue)
                except PilotException as error:
                    raise error
                else:
                    show_memory_usage()
                    # verify the job status on the server
                    #try:
                    #    job_status, job_attempt_nr, job_status_code = get_job_status_from_server(job.jobid, args.url, args.port)
                    #    if job_status == "running":
                    #        pilot_error_diag = "job %s is already running elsewhere - aborting" % (job.jobid)
                    #        logger.warning(pilot_error_diag)
                    #        raise JobAlreadyRunning(pilot_error_diag)
                    #except Exception as e:
                    #    logger.warning("%s" % e)
                # write time stamps to pilot timing file
                # note: PILOT_POST_GETJOB corresponds to START_TIME in Pilot 1
                add_to_pilot_timing(job.jobid, PILOT_PRE_GETJOB, time_pre_getjob, args)
                add_to_pilot_timing(job.jobid, PILOT_POST_GETJOB, time.time(), args)

                # add the job definition to the jobs queue and increase the job counter,
                # and wait until the job has finished
                put_in_queue(job, queues.jobs)

                jobnumber += 1
                while not args.graceful_stop.is_set():
                    if has_job_completed(queues, args):
                        #import signal
                        #os.kill(os.getpid(), signal.SIGTERM)

                        args.job_aborted.clear()
                        args.abort_job.clear()
                        logger.info('ready for new job')

                        # re-establish logging
                        logging.info('pilot has finished for previous job - re-establishing logging')
                        logging.handlers = []
                        logging.shutdown()
                        establish_logging(args)
                        pilot_version_banner()
                        getjob_requests = 0
                        add_to_pilot_timing('1', PILOT_MULTIJOB_START_TIME, time.time(), args)
                        break
                    time.sleep(0.5)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[job] retrieve thread has finished')


def print_node_info():
    """
    Print information about the local node to the log.

    :return:
    """

    if is_virtual_machine():
        logger.info("pilot is running in a virtual machine")
    else:
        logger.info("pilot is not running in a virtual machine")


def create_job(dispatcher_response, queue):
    """
    Create a job object out of the dispatcher response.

    :param dispatcher_response: raw job dictionary from the dispatcher.
    :param queue: queue name (string).
    :return: job object
    """

    # initialize (job specific) InfoService instance

    job = JobData(dispatcher_response)

    jobinfosys = InfoService()
    jobinfosys.init(queue, infosys.confinfo, infosys.extinfo, JobInfoProvider(job))
    job.init(infosys)

    #job.workdir = os.getcwd()

    logger.info('received job: %s (sleep until the job has finished)' % job.jobid)
    logger.info('job details: \n%s' % job)

    # payload environment wants the PANDAID to be set, also used below
    os.environ['PANDAID'] = job.jobid

    return job


def has_job_completed(queues, args):
    """
    Has the current job completed (finished or failed)?
    Note: the job object was extracted from monitored_payloads queue before this function was called.

    :param queues: Pilot queues object.
    :return: True is the payload has finished or failed
    """

    # check if the job has finished
    try:
        job = queues.completed_jobs.get(block=True, timeout=1)
    except queue.Empty:
        # logger.info("(job still running)")
        pass
    else:
        make_job_report(job)
        cmd = 'ls -lF %s' % os.environ.get('PILOT_HOME')
        logger.debug('%s:\n' % cmd)
        ec, stdout, stderr = execute(cmd)
        logger.debug(stdout)

        queue_report(queues)
        job.reset_errors()
        logger.info("job %s has completed (purged errors)" % job.jobid)

        # cleanup of any remaining processes
        if job.pid:
            job.zombies.append(job.pid)
        cleanup(job, args)

        return True

    # is there anything in the finished_jobs queue?
    #finished_queue_snapshot = list(queues.finished_jobs.queue)
    #peek = [obj for obj in finished_queue_snapshot if jobid == obj.jobid]
    #if peek:
    #    logger.info("job %s has completed (finished)" % jobid)
    #    return True

    # is there anything in the failed_jobs queue?
    #failed_queue_snapshot = list(queues.failed_jobs.queue)
    #peek = [obj for obj in failed_queue_snapshot if jobid == obj.jobid]
    #if peek:
    #    logger.info("job %s has completed (failed)" % jobid)
    #    return True

    return False


def get_job_from_queue(queues, state):
    """
    Check if the job has finished or failed and if so return it.

    :param queues: pilot queues.
    :param state: job state (e.g. finished/failed) (string).
    :return: job object.
    """
    try:
        if state == "finished":
            job = queues.finished_jobs.get(block=True, timeout=1)
        elif state == "failed":
            job = queues.failed_jobs.get(block=True, timeout=1)
        else:
            job = None
    except queue.Empty:
        # logger.info("(job still running)")
        job = None
    else:
        # make sure that state=failed
        set_pilot_state(job=job, state=state)
        logger.info("job %s has state=%s" % (job.jobid, job.state))

    return job


def is_queue_empty(queues, q):
    """
    Check if the given queue is empty (without pulling).

    :param queues: pilot queues object.
    :param q: queue name (string).
    :return: True if queue is empty, False otherwise
    """

    status = False
    if q in queues._fields:
        _q = getattr(queues, q)
        jobs = list(_q.queue)
        if len(jobs) > 0:
            logger.info('queue %s not empty: found %d job(s)' % (q, len(jobs)))
        else:
            logger.info('queue %s is empty' % q)
            status = True
    else:
        logger.warning('queue %s not present in %s' % (q, queues._fields))

    return status


def order_log_transfer(queues, job):
    """
    Order a log transfer for a failed job.

    :param queues: pilot queues object.
    :param job: job object.
    :return:
    """

    # add the job object to the data_out queue to have it staged out
    job.stageout = 'log'  # only stage-out log file
    #set_pilot_state(job=job, state='stageout')
    put_in_queue(job, queues.data_out)

    logger.debug('job added to data_out queue')

    # wait for the log transfer to finish
    n = 0
    nmax = 60
    while n < nmax:
        # refresh the log_transfer since it might have changed
        log_transfer = job.get_status('LOG_TRANSFER')
        logger.info('waiting for log transfer to finish (#%d/#%d): %s' % (n + 1, nmax, log_transfer))
        if is_queue_empty(queues, 'data_out') and \
                (log_transfer == LOG_TRANSFER_DONE or log_transfer == LOG_TRANSFER_FAILED):  # set in data component
            logger.info('stage-out of log has completed')
            break
        else:
            if log_transfer == LOG_TRANSFER_IN_PROGRESS:  # set in data component, job object is singleton
                logger.info('log transfer is in progress')
            time.sleep(2)
            n += 1

    logger.info('proceeding with server update (n=%d)' % n)


def wait_for_aborted_job_stageout(args, queues, job):
    """
    Wait for stage-out to finish for aborted job.

    :param args: pilot args object.
    :param queues: pilot queues object.
    :param job: job object.
    :return:
    """

    # if the pilot received a kill signal, how much time has passed since the signal was intercepted?
    try:
        time_since_kill = get_time_since('1', PILOT_KILL_SIGNAL, args)
        was_killed = was_pilot_killed(args.timing)
        if was_killed:
            logger.info('%d s passed since kill signal was intercepted - make sure that stage-out has finished' % time_since_kill)
    except Exception as e:
        logger.warning('exception caught: %s' % e)
        time_since_kill = 60
    else:
        if time_since_kill > 60 or time_since_kill < 0:  # fail-safe
            logger.warning('reset time_since_kill to 60 since value is out of allowed limits')
            time_since_kill = 60

    # if stage-out has not finished, we need to wait (less than two minutes or the batch system will issue
    # a hard SIGKILL)
    max_wait_time = 2 * 60 - time_since_kill - 5
    logger.debug('using max_wait_time = %d s' % max_wait_time)
    t0 = time.time()
    while time.time() - t0 < max_wait_time:
        if job in queues.finished_data_out.queue or job in queues.failed_data_out.queue:
            logger.info('stage-out has finished, proceed with final server update')
            break
        else:
            time.sleep(0.5)

    logger.info('proceeding with final server update')


def get_job_status(job, key):
    """
    Wrapper function around job.get_status().
    If key = 'LOG_TRANSFER' but job object is not defined, the function will return value = LOG_TRANSFER_NOT_DONE.

    :param job: job object.
    :param key: key name (string).
    :return: value (string).
    """

    value = ""
    if job:
        value = job.get_status(key)
    else:
        if key == 'LOG_TRANSFER':
            value = LOG_TRANSFER_NOT_DONE

    return value


def queue_monitor(queues, traces, args):  # noqa: C901
    """
    Monitoring of queues.
    This function monitors queue activity, specifically if a job has finished or failed and then reports to the server.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
    """

    # scan queues until at least one queue has a job object. abort if it takes too long time
    if not scan_for_jobs(queues):
        logger.warning('queues are still empty of jobs - will begin queue monitoring anyway')

    job = None
    while True:  # will abort when graceful_stop has been set or if enough time has passed after kill signal
        time.sleep(1)

        if traces.pilot['command'] == 'abort':
            logger.warning('job queue monitor received an abort instruction')
            args.graceful_stop.set()

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        # (abort at the end of the loop)
        abort_thread = should_abort(args, label='job:queue_monitor')
        if abort_thread and os.environ.get('PILOT_WRAP_UP', '') == 'NORMAL':
            pause_queue_monitor(20)

        # check if the job has finished
        imax = 20
        i = 0
        while i < imax and os.environ.get('PILOT_WRAP_UP', '') == 'NORMAL':
            job = get_finished_or_failed_job(args, queues)
            if job:
                logger.debug('returned job has state=%s' % job.state)
                #if job.state == 'failed':
                #    logger.warning('will abort failed job (should prepare for final server update)')
                break
            i += 1
            state = get_pilot_state()  # the job object is not available, but the state is also kept in PILOT_JOB_STATE
            if state != 'stage-out':
                # logger.info("no need to wait since job state=\'%s\'" % state)
                break
            pause_queue_monitor(1) if not abort_thread else pause_queue_monitor(10)

        # job has not been defined if it's still running
        if not job and not abort_thread:
            continue

        completed_jobids = queues.completed_jobids.queue if queues.completed_jobids else []
        if job and job.jobid not in completed_jobids:
            logger.info("preparing for final server update for job %s in state=\'%s\'" % (job.jobid, job.state))

            if args.job_aborted.is_set():
                # wait for stage-out to finish for aborted job
                wait_for_aborted_job_stageout(args, queues, job)

            # send final server update
            update_server(job, args)

            # we can now stop monitoring this job, so remove it from the monitored_payloads queue and add it to the
            # completed_jobs queue which will tell retrieve() that it can download another job
            try:
                _job = queues.monitored_payloads.get(block=True, timeout=1)
            except queue.Empty:
                logger.warning('failed to dequeue job: queue is empty (did job fail before job monitor started?)')
                make_job_report(job)
            else:
                logger.debug('job %s was dequeued from the monitored payloads queue' % _job.jobid)
                # now ready for the next job (or quit)
                put_in_queue(job.jobid, queues.completed_jobids)

                put_in_queue(job, queues.completed_jobs)
                del _job
                logger.debug('tmp job object deleted')

        if abort_thread:
            break

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[job] queue monitor thread has finished')


def update_server(job, args):
    """
    Update the server (wrapper for send_state() that also prepares the metadata).

    :param job: job object.
    :param args: pilot args object.
    :return:
    """

    # user specific actions
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
    metadata = user.get_metadata(job.workdir)
    try:
        user.update_server(job)
    except Exception as e:
        logger.warning('exception caught in update_server(): %s' % e)
    if job.fileinfo:
        send_state(job, args, job.state, xml=dumps(job.fileinfo), metadata=metadata)
    else:
        send_state(job, args, job.state, metadata=metadata)


def pause_queue_monitor(delay):
    """
    Pause the queue monitor to let log transfer complete.
    Note: this function should use globally available object. Use sleep for now.
    :param delay: sleep time in seconds (int).
    :return:
    """

    logger.warning('since job:queue_monitor is responsible for sending job updates, we sleep for %d s' % delay)
    time.sleep(delay)


def get_finished_or_failed_job(args, queues):
    """
    Check if the job has either finished or failed and if so return it.
    If failed, order a log transfer. If the job is in state 'failed' and abort_job is set, set job_aborted.

    :param args: pilot args object.
    :param queues: pilot queues object.
    :return: job object.
    """

    job = get_job_from_queue(queues, "finished")
    if job:
        # logger.debug('get_finished_or_failed_job: job has finished')
        pass
    else:
        # logger.debug('check_job: job has not finished')
        job = get_job_from_queue(queues, "failed")
        if job:
            logger.debug('get_finished_or_failed_job: job has failed')
            job.state = 'failed'
            args.job_aborted.set()

            # get the current log transfer status
            log_transfer = get_job_status(job, 'LOG_TRANSFER')
            if log_transfer == LOG_TRANSFER_NOT_DONE:
                # order a log transfer for a failed job
                order_log_transfer(queues, job)

    # check if the job has failed
    if job and job.state == 'failed':
        # set job_aborted in case of kill signals
        if args.abort_job.is_set():
            logger.warning('queue monitor detected a set abort_job (due to a kill signal)')
            # do not set graceful stop if pilot has not finished sending the final job update
            # i.e. wait until SERVER_UPDATE is DONE_FINAL
            #check_for_final_server_update(args.update_server)
            #args.job_aborted.set()

    return job


def get_heartbeat_period(debug=False):
    """
    Return the proper heartbeat period, as determined by normal or debug mode.
    In normal mode, the heartbeat period is 30*60 s, while in debug mode it is 5*60 s. Both values are defined in the
    config file.

    :param debug: Boolean, True for debug mode. False otherwise.
    :return: heartbeat period (int).
    """

    try:
        return int(config.Pilot.heartbeat if not debug else config.Pilot.debug_heartbeat)
    except Exception as e:
        logger.warning('bad config data for heartbeat period: %s (will use default 1800 s)' % e)
        return 1800


def check_for_abort_job(args, caller=''):
    """
    Check if args.abort_job.is_set().

    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :param caller: function name of caller (string).
    :return: Boolean, True if args_job.is_set()
    """
    abort_job = False
    if args.abort_job.is_set():
        logger.warning('%s detected an abort_job request (signal=%s)' % (caller, args.signal))
        logger.warning('in case pilot is running more than one job, all jobs will be aborted')
        abort_job = True

    return abort_job


def interceptor(queues, traces, args):
    """
    MOVE THIS TO INTERCEPTOR.PY; TEMPLATE FOR THREADS

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
    """

    # overall loop counter (ignoring the fact that more than one job may be running)
    n = 0
    while not args.graceful_stop.is_set():
        time.sleep(0.1)

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        # (abort at the end of the loop)
        abort = should_abort(args, label='job:interceptor')

        # check for any abort_job requests
        abort_job = check_for_abort_job(args, caller='interceptor')
        if not abort_job:
            # peek at the jobs in the validated_jobs queue and send the running ones to the heartbeat function
            jobs = queues.monitored_payloads.queue
            if jobs:
                for i in range(len(jobs)):

                    logger.info('interceptor loop %d: looking for communication file' % n)
            time.sleep(30)

        n += 1

        if abort or abort_job:
            break

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[job] interceptor thread has finished')


def job_monitor(queues, traces, args):  # noqa: C901
    """
    Monitoring of job parameters.
    This function monitors certain job parameters, such as job looping, at various time intervals. The main loop
    is executed once a minute, while individual verifications may be executed at any time interval (>= 1 minute). E.g.
    looping jobs are checked once per ten minutes (default) and the heartbeat is send once per 30 minutes. Memory
    usage is checked once a minute.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
    """

    # initialize the monitoring time object
    mt = MonitoringTime()

    # peeking and current time; peeking_time gets updated if and when jobs are being monitored, update_time is only
    # used for sending the heartbeat and is updated after a server update
    peeking_time = int(time.time())
    update_time = peeking_time

    # overall loop counter (ignoring the fact that more than one job may be running)
    n = 0
    while not args.graceful_stop.is_set():
        time.sleep(0.5)

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        # (abort at the end of the loop)
        abort = should_abort(args, label='job:job_monitor')

        if traces.pilot.get('command') == 'abort':
            logger.warning('job monitor received an abort command')

        # check for any abort_job requests
        abort_job = check_for_abort_job(args, caller='job monitor')
        if not abort_job:
            if not queues.current_data_in.empty():
                # make sure to send heartbeat regularly if stage-in takes a long time
                jobs = queues.current_data_in.queue
                if jobs:
                    for i in range(len(jobs)):
                        # send heartbeat if it is time (note that the heartbeat function might update the job object, e.g.
                        # by turning on debug mode, ie we need to get the heartbeat period in case it has changed)
                        update_time = send_heartbeat_if_time(jobs[i], args, update_time)

                        # note: when sending a state change to the server, the server might respond with 'tobekilled'
                        if jobs[i].state == 'failed':
                            logger.warning('job state is \'failed\' - order log transfer and abort job_monitor() (1)')
                            jobs[i].stageout = 'log'  # only stage-out log file
                            put_in_queue(jobs[i], queues.data_out)

                    # sleep for a while if stage-in has not completed
                    time.sleep(1)
                    continue
            elif queues.finished_data_in.empty():
                # sleep for a while if stage-in has not completed
                time.sleep(1)
                continue

            time.sleep(60)

        # peek at the jobs in the validated_jobs queue and send the running ones to the heartbeat function
        jobs = queues.monitored_payloads.queue
        if jobs:
            # update the peeking time
            peeking_time = int(time.time())
            for i in range(len(jobs)):
                current_id = jobs[i].jobid
                logger.info('monitor loop #%d: job %d:%s is in state \'%s\'' % (n, i, current_id, jobs[i].state))
                if jobs[i].state == 'finished' or jobs[i].state == 'failed':
                    logger.info('will abort job monitoring soon since job state=%s (job is still in queue)' % jobs[i].state)
                    break

                # perform the monitoring tasks
                exit_code, diagnostics = job_monitor_tasks(jobs[i], mt, args)
                if exit_code != 0:
                    try:
                        fail_monitored_job(jobs[i], exit_code, diagnostics, queues, traces)
                    except Exception as e:
                        logger.warning('(1) exception caught: %s (job id=%s)' % (e, current_id))
                    break

                # run this check again in case job_monitor_tasks() takes a long time to finish (and the job object
                # has expired in the mean time)
                try:
                    _job = jobs[i]
                except Exception:
                    logger.info('aborting job monitoring since job object (job id=%s) has expired' % current_id)
                    break

                # send heartbeat if it is time (note that the heartbeat function might update the job object, e.g.
                # by turning on debug mode, ie we need to get the heartbeat period in case it has changed)
                try:
                    update_time = send_heartbeat_if_time(_job, args, update_time)
                except Exception as e:
                    logger.warning('(2) exception caught: %s (job id=%s)' % (e, current_id))
                    break
                else:
                    # note: when sending a state change to the server, the server might respond with 'tobekilled'
                    if _job.state == 'failed':
                        logger.warning('job state is \'failed\' - order log transfer and abort job_monitor() (2)')
                        _job.stageout = 'log'  # only stage-out log file
                        put_in_queue(_job, queues.data_out)
                        abort = True
                        break

        elif os.environ.get('PILOT_JOB_STATE') == 'stagein':
            logger.info('job monitoring is waiting for stage-in to finish')
        else:
            # check the waiting time in the job monitor. set global graceful_stop if necessary
            check_job_monitor_waiting_time(args, peeking_time, abort_override=abort_job)

        n += 1

        if abort or abort_job:
            break

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[job] job monitor thread has finished')


def send_heartbeat_if_time(job, args, update_time):
    """
    Send a heartbeat to the server if it is time to do so.

    :param job: job object.
    :param args: args object.
    :param update_time: last update time (from time.time()).
    :return: possibly updated update_time (from time.time()).
    """

    if int(time.time()) - update_time >= get_heartbeat_period(job.debug):
        if job.serverstate != 'finished' and job.serverstate != 'failed':
            send_state(job, args, 'running')
            update_time = int(time.time())

    return update_time


def check_job_monitor_waiting_time(args, peeking_time, abort_override=False):
    """
    Check the waiting time in the job monitor.
    Set global graceful_stop if necessary.

    :param args: args object.
    :param peeking_time: time when monitored_payloads queue was peeked into (int).
    :return:
    """

    waiting_time = int(time.time()) - peeking_time
    msg = 'no jobs in monitored_payloads queue (waited for %d s)' % waiting_time
    if waiting_time > 60 * 60:
        abort = True
        msg += ' - aborting'
    else:
        abort = False
    if logger:
        logger.warning(msg)
    else:
        print(msg)
    if abort or abort_override:
        # do not set graceful stop if pilot has not finished sending the final job update
        # i.e. wait until SERVER_UPDATE is DONE_FINAL
        check_for_final_server_update(args.update_server)
        args.graceful_stop.set()


def fail_monitored_job(job, exit_code, diagnostics, queues, traces):
    """
    Fail a monitored job.

    :param job: job object
    :param exit_code: exit code from job_monitor_tasks (int).
    :param diagnostics: pilot error diagnostics (string).
    :param queues: queues object.
    :param traces: traces object.
    :return:
    """

    set_pilot_state(job=job, state="failed")
    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code)
    job.pilorerrordiag = diagnostics
    traces.pilot['error_code'] = exit_code
    put_in_queue(job, queues.failed_payloads)
    logger.info('aborting job monitoring since job state=%s' % job.state)


def make_job_report(job):
    """
    Make a summary report for the given job.
    This function is called when the job has completed.

    :param job: job object.
    :return:
    """

    logger.info('')
    logger.info('job summary report')
    logger.info('--------------------------------------------------')
    logger.info('PanDA job id: %s' % job.jobid)
    logger.info('task id: %s' % job.taskid)
    n = len(job.piloterrorcodes)
    if n > 0:
        for i in range(n):
            logger.info('error %d/%d: %s: %s' % (i + 1, n, job.piloterrorcodes[i], job.piloterrordiags[i]))
    else:
        logger.info('errors: (none)')
    if job.piloterrorcode != 0:
        logger.info('pilot error code: %d' % job.piloterrorcode)
        logger.info('pilot error diag: %s' % job.piloterrordiag)
    info = ""
    for key in job.status:
        info += key + " = " + job.status[key] + " "
    logger.info('status: %s' % info)
    s = ""
    if job.is_analysis() and job.state != 'finished':
        s = '(user job is recoverable)' if errors.is_recoverable(code=job.piloterrorcode) else '(user job is not recoverable)'
    logger.info('pilot state: %s %s' % (job.state, s))
    logger.info('transexitcode: %d' % job.transexitcode)
    logger.info('exeerrorcode: %d' % job.exeerrorcode)
    logger.info('exeerrordiag: %s' % job.exeerrordiag)
    logger.info('exitcode: %d' % job.exitcode)
    logger.info('exitmsg: %s' % job.exitmsg)
    logger.info('cpuconsumptiontime: %d %s' % (job.cpuconsumptiontime, job.cpuconsumptionunit))
    logger.info('nevents: %d' % job.nevents)
    logger.info('neventsw: %d' % job.neventsw)
    logger.info('pid: %s' % job.pid)
    logger.info('pgrp: %s' % str(job.pgrp))
    logger.info('corecount: %d' % job.corecount)
    logger.info('event service: %s' % str(job.is_eventservice))
    logger.info('sizes: %s' % str(job.sizes))
    logger.info('--------------------------------------------------')
    logger.info('')
