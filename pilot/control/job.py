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

from __future__ import print_function

import os
import sys
import time
import hashlib

try:
    import Queue as queue
except Exception:
    import queue  # python 3

from json import dumps

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import ExcThread, PilotException
from pilot.info import infosys, JobData
from pilot.util import https
from pilot.util.auxiliary import time_stamp, get_batchsystem_jobid, get_job_scheduler_id, get_pilot_id, get_logger
from pilot.util.config import config
from pilot.util.constants import PILOT_PRE_GETJOB, PILOT_POST_GETJOB
from pilot.util.filehandling import get_files, tail
from pilot.util.harvester import request_new_jobs, remove_job_request_file
from pilot.util.jobmetrics import get_job_metrics
from pilot.util.monitoring import job_monitor_tasks, check_local_space
from pilot.util.monitoringtime import MonitoringTime
from pilot.util.proxy import get_distinguished_name
from pilot.util.timing import add_to_pilot_timing, get_getjob_time, get_setup_time, get_stagein_time, get_stageout_time,\
    get_payload_execution_time, get_initial_setup_time, get_postgetjob_time
from pilot.util.workernode import get_disk_space, collect_workernode_info, get_node_name, is_virtual_machine

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def control(queues, traces, args):
    """
    (add description)

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
                         name=name) for name, target in targets.items()]

    [thread.start() for thread in threads]

    logger.info('waiting for interrupts')

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


def _validate_job(job):
    """
    (add description)

    :param job: job object.
    :return:
    """
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('%s: job did not validate correctly -- skipping' % job.jobid)
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'job failed random validation'
    #     return False
    return True


def send_state(job, args, state, xml=None):
    """
    Update the server (send heartbeat message).
    Interpret and handle any server instructions arriving with the updateJob backchannel.

    :param job: job object.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :param state: job state (string).
    :param xml: optional metadata xml (string).
    :return: boolean (True if successful, False otherwise).
    """

    log = get_logger(job.jobid)

    # should the pilot make any server updates?
    if not args.update_server:
        log.info('pilot will not update the server')
        return True

    if state == 'finished' or state == 'failed':
        log.info('job %s has %s - sending final server update' % (job.jobid, state))
    else:
        log.info('job %s has state \'%s\' - sending heartbeat' % (job.jobid, state))

    # build the data structure needed for getJob, updateJob
    data = get_data_structure(job, state, args.site, args.version_tag, xml=xml)

    try:
        if args.url != '' and args.port != 0:
            pandaserver = args.url + ':' + str(args.port)
        else:
            pandaserver = config.Pilot.pandaserver

        if config.Pilot.pandajob == 'real':
            res = https.request('{pandaserver}/server/panda/updateJob'.format(pandaserver=pandaserver), data=data)
            if res is not None:
                log.info('server updateJob request completed for job %s' % job.jobid)
                log.info('res = %s' % str(res))

                # does the server update contain any backchannel information? if so, update the job object
                if 'command' in res and res.get('command') != 'NULL':
                    # look for 'tobekilled', 'softkill', 'debug', 'debugoff'
                    if res.get('command') == 'tobekilled':
                        log.info('pilot received a panda server signal to kill job %d at %s' %
                                 (job.jobid, time_stamp()))
                    elif res.get('command') == 'softkill':
                        log.info('pilot received a panda server signal to softkill job %d at %s' %
                                 (job.jobid, time_stamp()))
                    elif res.get('command') == 'debug':
                        log.info('pilot received a command to turn on debug mode from the server')
                    elif res.get('command') == 'debugoff':
                        log.info('pilot received a command to turn off debug mode from the server')
                    else:
                        log.warning('received unknown server command via backchannel: %s' % res.get('command'))

                return True
        else:
            log.info('skipping job update for fake test job')
            return True
    except Exception as e:
        log.warning('while setting job state, Exception caught: %s' % str(e.message))
        pass

    log.warning('set job state=%s failed' % state)
    return False


def get_data_structure(job, state, sitename, versiontag, xml=None):
    """
    Build the data structure needed for getJob, updateJob.

    :param job: job object.
    :param state: state of the job (string).
    :param sitename: site name (string).
    :param versiontag: version tag (string).
    :param xml: optional XML string.
    :return: data structure (dictionary).
    """

    log = get_logger(job.jobid)

    data = {'jobId': job.jobid,
            'state': state,
            'timestamp': time_stamp(),
            'siteName': sitename,
            'node': get_node_name()}

    # error codes
    pilot_error_code = job.piloterrorcode
    pilot_error_codes = job.piloterrorcodes
    if pilot_error_codes != []:
        log.warning('pilotErrorCodes = %s (will report primary/first error code)' % str(pilot_error_codes))
        data['pilotErrorCode'] = pilot_error_codes[0]
    else:
        data['pilotErrorCode'] = pilot_error_code

    pilot_error_diag = job.piloterrordiag
    pilot_error_diags = job.piloterrordiags
    if pilot_error_diags != []:
        log.warning('pilotErrorDiags = %s (will report primary/first error diag)' % str(pilot_error_diags))
        data['pilotErrorDiag'] = pilot_error_diags[0]
    else:
        data['pilotErrorDiag'] = pilot_error_diag

    data['transExitCode'] = job.transexitcode
    data['exeErrorCode'] = job.exeerrorcode
    data['exeErrorDiag'] = job.exeerrordiag

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
                              (pilotid, batchsystem_type, versiontag, pilotversion)
            data['batchID'] = batchsystem_id,
        else:
            data['pilotID'] = "%s|%s|%s" % (pilotid, versiontag, pilotversion)

    starttime = get_postgetjob_time(job.jobid)
    if starttime:
        data['startTime'] = starttime

    job_metrics = get_job_metrics(job)
    if job_metrics:
        data['jobMetrics'] = job_metrics

    if xml is not None:
        data['xml'] = xml

    # in debug mode, also send a tail of the latest log file touched by the payload
    if job.debug:
        # find the latest updated log file
        list_of_files = get_files()
        latest_file = max(list_of_files, key=os.path.getctime)
        log.info('tail of file %s will be added to heartbeat' % latest_file)

        # now get the tail of the found log file and protect against potentially large tails
        stdout_tail = tail(latest_file)
        stdout_tail = stdout_tail[-2048:]
        data['stdout'] = stdout_tail

    if state == 'finished' or state == 'failed':
        # collect pilot timing data
        time_getjob = get_getjob_time(job.jobid)
        time_initial_setup = get_initial_setup_time(job.jobid)
        time_setup = get_setup_time(job.jobid)
        time_total_setup = time_initial_setup + time_setup
        time_stagein = get_stagein_time(job.jobid)
        time_payload = get_payload_execution_time(job.jobid)
        time_stageout = get_stageout_time(job.jobid)
        log.info('.' * 30)
        log.info('. Timing measurements:')
        log.info('. get job = %d s' % time_getjob)
        log.info('. initial setup = %d s' % time_initial_setup)
        log.info('. payload setup = %d s' % time_setup)
        log.info('. total setup = %d s' % time_total_setup)
        log.info('. stage-in = %d s' % time_stagein)
        log.info('. payload execution = %d s' % time_payload)
        log.info('. stage-out = %d s' % time_stageout)
        log.info('.' * 30)

        data['pilotTiming'] = "%s|%s|%s|%s|%s" % \
                              (time_getjob, time_stagein, time_payload, time_stageout, time_total_setup)

    return data


def validate(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        try:
            job = queues.jobs.get(block=True, timeout=1)
        except queue.Empty:
            continue

        log = get_logger(job.jobid)
        traces.pilot['nr_jobs'] += 1

        # set the environmental variable for the task id
        os.environ['PanDA_TaskID'] = str(job.taskid)
        log.info('processing PanDA job %s from task %s' % (job.jobid, job.taskid))

        if _validate_job(job):

            log.debug('creating job working directory')
            job_dir = os.path.join(args.mainworkdir, 'PanDA_Pilot-%s' % job.jobid)
            try:
                os.mkdir(job_dir)
                job.workdir = job_dir
            except Exception as e:
                log.debug('cannot create working directory: %s' % str(e))
                queues.failed_jobs.put(job)
                break

            log.debug('symlinking pilot log')
            try:
                os.symlink('../pilotlog.txt', os.path.join(job_dir, 'pilotlog.txt'))
            except Exception as e:
                log.debug('cannot symlink pilot log: %s' % str(e))
#                queues.failed_jobs.put(job)
#                break

            queues.validated_jobs.put(job)
        else:
            log.debug('Failed to validate job=%s' % job.jobid)
            queues.failed_jobs.put(job)


def create_data_payload(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    while not args.graceful_stop.is_set():
        try:
            job = queues.validated_jobs.get(block=True, timeout=1)
        except queue.Empty:
            continue

        if job.indata:
            # if the job has input data, put the job object in the data_in queue which will trigger stage-in
            queues.data_in.put(job)
        else:
            # if the job does not have any input data, then pretend that stage-in has finished and put the job
            # in the finished_data_in queue
            queues.finished_data_in.put(job)
        queues.payloads.put(job)


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

    :param args: pilot args object.
    :return: job_label (string).
    """

    if args.version_tag.startswith('RC'):
        job_label = 'rc_test2'
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

    #_diskspace = get_disk_space(args.location.queuedata)

    ## passing here the queuedata is redundant since it's available
    ## globally via pilot.info.infosys
    ## kept for a while as "wrong" example .. to be cleaned soon
    _diskspace = get_disk_space(args.info.infoservice.queuedata)

    _mem, _cpu, _disk = collect_workernode_info()
    _nodename = get_node_name()

    # override for RC dev pilots
    job_label = get_job_label(args)

    data = {
        'siteName': args.resource,
        'computingElement': args.location.queue,
        'prodSourceLabel': job_label,
        'diskSpace': _diskspace,
        'workingGroup': args.working_group,
        'cpu': _cpu,
        'mem': _mem,
        'node': _nodename
    }

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

    return data


def proceed_with_getjob(timefloor, starttime, jobnumber, getjob_requests, harvester, verify_proxy):
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
    :return: Boolean.
    """

    time.sleep(10)

    # use for testing thread exceptions. the exception will be picked up by ExcThread run() and caught in job.control()
    # raise NoLocalSpace('testing exception from proceed_with_getjob')

    currenttime = time.time()

    # should the proxy be verified?
    if verify_proxy:
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        userproxy = __import__('pilot.user.%s.proxy' % pilot_user, globals(), locals(), [pilot_user], -1)

        # is the proxy still valid?
        exit_code, diagnostics = userproxy.verify_proxy()
        if exit_code == errors.NOPROXY or exit_code == errors.NOVOMSPROXY:
            logger.warning(diagnostics)
            return False

    # is there enough local space to run a job?
    ec, diagnostics = check_local_space()
    if ec != 0:
        return False

    if harvester:
        maximum_getjob_requests = 60  # 1 s apart
    else:
        maximum_getjob_requests = config.Pilot.maximum_getjob_requests

    if getjob_requests > int(maximum_getjob_requests):
        logger.warning('reached maximum number of getjob requests (%s) -- will abort pilot' %
                       config.Pilot.maximum_getjob_requests)
        return False

    if timefloor == 0 and jobnumber > 0:
        logger.warning("since timefloor is set to 0, pilot was only allowed to run one job")
        return False

    if (currenttime - starttime > timefloor) and jobnumber > 0:
        logger.warning("the pilot has run out of time (timefloor=%d has been passed)" % timefloor)
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

    return True


def getjob_server_command(url, port):
    """
    Prepare the getJob server command.

    :param url: PanDA server URL (string)
    :param port: PanDA server port
    :return: full server command (URL string)
    """

    if url != "":
        url = url + ':%s' % port  # port is always set
    else:
        url = config.Pilot.pandaserver
    if url == "":
        logger.fatal('PanDA server url not set (either as pilot option or in config file)')
    elif not url.startswith("https://"):
        url = 'https://' + url
        logger.warning('detected missing protocol in server url (added)')

    return '{pandaserver}/server/panda/getJob'.format(pandaserver=url)


# MOVE TO EVENT SERVICE CODE AFTER MERGE
def create_es_file_dictionary(writetofile):
    """
    Create the event range file dictionary from the writetofile info.

    writetofile = 'fileNameForTrf_1:LFN_1,LFN_2^fileNameForTrf_2:LFN_3,LFN_4'
    -> esfiledictionary = {'fileNameForTrf_1': 'LFN_1,LFN_2', 'fileNameForTrf_2': 'LFN_3,LFN_4'}
    Also, keep track of the dictionary keys (e.g. 'fileNameForTrf_1') ordered since we have to use them to update the
    jobParameters once we know the full path to them (i.e. '@fileNameForTrf_1:..' will be replaced by
    '@/path/filename:..'), the dictionary is otherwise not ordered so we cannot simply use the dictionary keys later.
    fileinfo = ['fileNameForTrf_1:LFN_1,LFN_2', 'fileNameForTrf_2:LFN_3,LFN_4']

    :param writetofile: file info string
    :return: esfiledictionary, orderedfnamelist
    """

    fileinfo = writetofile.split("^")
    esfiledictionary = {}
    orderedfnamelist = []
    for i in range(len(fileinfo)):
        # Extract the file name
        if ":" in fileinfo[i]:
            finfo = fileinfo[i].split(":")

            # fix the issue that some athena 20 releases have _000 at the end of the filename
            if finfo[0].endswith("_000"):
                finfo[0] = finfo[0][:-4]
            esfiledictionary[finfo[0]] = finfo[1]
            orderedfnamelist.append(finfo[0])
        else:
            logger.warning("file info does not have the correct format, expected a separator \':\': %s" % (fileinfo[i]))
            esfiledictionary = {}
            break

    return esfiledictionary, orderedfnamelist


# MOVE TO EVENT SERVICE CODE AFTER MERGE
def update_es_guids(guids):
    """
    Update the NULL valued ES guids.
    This is necessary since guids are used as dictionary keys in some places.
    Replace the NULL values with different values:
    E.g. guids = 'NULL,NULL,NULL,sasdasdasdasdd'
    -> 'DUMMYGUID0,DUMMYGUID1,DUMMYGUID2,sasdasdasdasdd'

    :param guids:
    :return: updated guids
    """

    for i in range(guids.count('NULL')):
        guids = guids.replace('NULL', 'DUMMYGUID%d' % i, 1)

    return guids


# MOVE TO EVENT SERVICE CODE AFTER MERGE
def update_es_dispatcher_data(data):
    """
    Update the dispatcher data for Event Service.
    For Event Service merge jobs, the input file list will not arrive in the inFiles list as usual, but in the
    writeToFile field, so the inFiles list need to be corrected.

    :param data: dispatcher data dictionary
    :return: data (updated dictionary)
    """

    if 'writeToFile' in data:
        writetofile = data['writeToFile']
        esfiledictionary, orderedfnamelist = create_es_file_dictionary(writetofile)
        if esfiledictionary != {}:
            # fix the issue that some athena 20 releases have _000 at the end of the filename
            for name in orderedfnamelist:
                name_000 = "@%s_000 " % (name)
                new_name = "@%s " % (name)
                if name_000 in data['jobPars']:
                    data['jobPars'] = data['jobPars'].replace(name_000, new_name)

            # Remove the autoconf
            if "--autoConfiguration=everything " in data['jobPars']:
                data['jobPars'] = data['jobPars'].replace("--autoConfiguration=everything ", " ")

            # Replace the NULL valued guids for the ES files
            data['GUID'] = update_es_guids(data['GUID'])
        else:
            logger.warning("empty event service file dictionary")

    return data


def get_job_definition_from_file(path, harvester):
    """
    Get a job definition from a pre-placed file.
    In Harvester mode, also remove any existing job request files since it is no longer needed/wanted.

    :param path: path to job definition file.
    :param harvester: True if Harvester is being used (determined from args.harvester), otherwise False
    :return: job definition dictionary.
    """

    # remove any existing Harvester job request files (silent in non-Harvester mode)
    if harvester:
        remove_job_request_file()

    res = {}
    with open(path, 'r') as jobdatafile:
        response = jobdatafile.read()
        if len(response) == 0:
            logger.fatal('encountered empty job definition file: %s' % path)
            res = None  # this is a fatal error, no point in continuing as the file will not be replaced
        else:
            # parse response message
            from urlparse import parse_qsl
            datalist = parse_qsl(response, keep_blank_values=True)

            # convert to dictionary
            for d in datalist:
                res[d[0]] = d[1]

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


def get_job_definition(args):
    """
    Get a job definition from a source (server or pre-placed local file).
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return: job definition dictionary.
    """

    res = {}

    path = os.path.join(os.environ['PILOT_WORK_DIR'], config.Pilot.pandajobdata)

    if not os.path.exists(path):
        logger.warning('Job definition file does not exist: %s' % path)

    # should we run a norma 'real' job or with a 'fake' job?
    if config.Pilot.pandajob == 'fake':
        logger.info('will use a fake PanDA job')
        res = get_fake_job()
    elif os.path.exists(path):
        logger.info('will read job definition from file %s' % path)
        res = get_job_definition_from_file(path, args.harvester)
    else:
        if args.harvester:
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
        res = {u'jobsetID': u'NULL',
               u'logGUID': log_guid,
               u'cmtConfig': u'x86_64-slc6-gcc48-opt',
               u'prodDBlocks': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
               u'dispatchDBlockTokenForOut': u'NULL,NULL',
               u'destinationDBlockToken': u'NULL,NULL',
               u'destinationSE': u'AGLT2_TEST',
               u'realDatasets': job_name,
               u'prodUserID': u'no_one',
               u'GUID': guid,
               u'realDatasetsIn': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
               u'nSent': 0,
               u'cloud': u'US',
               u'StatusCode': 0,
               u'homepackage': u'AtlasProduction/20.1.4.14',
               u'inFiles': u'HITS.06828093._000096.pool.root.1',
               u'processingType': u'pilot-ptest',
               u'ddmEndPointOut': u'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
               u'fsize': u'94834717',
               u'fileDestinationSE': u'AGLT2_TEST,AGLT2_TEST',
               u'scopeOut': u'panda',
               u'minRamCount': 0,
               u'jobDefinitionID': 7932,
               u'maxWalltime': u'NULL',
               u'scopeLog': u'panda',
               u'transformation': u'Reco_tf.py',
               u'maxDiskCount': 0,
               u'coreCount': 1,
               u'prodDBlockToken': u'NULL',
               u'transferType': u'NULL',
               u'destinationDblock': job_name,
               u'dispatchDBlockToken': u'NULL',
               u'jobPars': u'--maxEvents=1 --inputHITSFile HITS.06828093._000096.pool.root.1 --outputRDOFile RDO_%s.root' % job_name,
               u'attemptNr': 0,
               u'swRelease': u'Atlas-20.1.4',
               u'nucleus': u'NULL',
               u'maxCpuCount': 0,
               u'outFiles': u'RDO_%s.root,%s.job.log.tgz' % (job_name, job_name),
               u'currentPriority': 1000,
               u'scopeIn': u'mc15_13TeV',
               u'PandaID': '0',
               u'sourceSite': u'NULL',
               u'dispatchDblock': u'NULL',
               u'prodSourceLabel': u'ptest',
               u'checksum': u'ad:5d000974',
               u'jobName': job_name,
               u'ddmEndPointIn': u'UTA_SWT2_DATADISK',
               u'taskID': u'NULL',
               u'logFile': u'%s.job.log.tgz' % job_name}
    elif config.Pilot.testjobtype == 'user':
        logger.info('creating fake test user job definition')
        res = {u'jobsetID': u'NULL',
               u'logGUID': log_guid,
               u'cmtConfig': u'x86_64-slc6-gcc49-opt',
               u'prodDBlocks': u'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               u'dispatchDBlockTokenForOut': u'NULL,NULL',
               u'destinationDBlockToken': u'NULL,NULL',
               u'destinationSE': u'ANALY_SWT2_CPB',
               u'realDatasets': job_name,
               u'prodUserID': u'noone',
               u'GUID': guid,
               u'realDatasetsIn': u'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               u'nSent': u'0',
               u'cloud': u'US',
               u'StatusCode': 0,
               u'homepackage': u'AnalysisTransforms-AtlasDerivation_20.7.6.4',
               u'inFiles': u'AOD.07709524._000050.pool.root.1',
               u'processingType': u'pilot-ptest',
               u'ddmEndPointOut': u'SWT2_CPB_SCRATCHDISK,SWT2_CPB_SCRATCHDISK',
               u'fsize': u'1564780952',
               u'fileDestinationSE': u'ANALY_SWT2_CPB,ANALY_SWT2_CPB',
               u'scopeOut': u'user.gangarbt',
               u'minRamCount': u'0',
               u'jobDefinitionID': u'9445',
               u'maxWalltime': u'NULL',
               u'scopeLog': u'user.gangarbt',
               u'transformation': u'http://pandaserver.cern.ch:25080/trf/user/runAthena-00-00-11',
               u'maxDiskCount': u'0',
               u'coreCount': u'1',
               u'prodDBlockToken': u'NULL',
               u'transferType': u'NULL',
               u'destinationDblock': job_name,
               u'dispatchDBlockToken': u'NULL',
               u'jobPars': u'-a sources.20115461.derivation.tgz -r ./ -j "Reco_tf.py '
                           u'--inputAODFile AOD.07709524._000050.pool.root.1 --outputDAODFile test.pool.root '
                           u'--reductionConf HIGG3D1" -i "[\'AOD.07709524._000050.pool.root.1\']" -m "[]" -n "[]" --trf'
                           u' --useLocalIO --accessmode=copy -o '
                           u'"{\'IROOT\': [(\'DAOD_HIGG3D1.test.pool.root\', \'%s.root\')]}" '
                           u'--sourceURL https://aipanda012.cern.ch:25443' % (job_name),
               u'attemptNr': u'0',
               u'swRelease': u'Atlas-20.7.6',
               u'nucleus': u'NULL',
               u'maxCpuCount': u'0',
               u'outFiles': u'%s.root,%s.job.log.tgz' % (job_name, job_name),
               u'currentPriority': u'1000',
               u'scopeIn': u'data15_13TeV',
               u'PandaID': u'0',
               u'sourceSite': u'NULL',
               u'dispatchDblock': u'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               u'prodSourceLabel': u'ptest',
               u'checksum': u'ad:b11f45a7',
               u'jobName': job_name,
               u'ddmEndPointIn': u'SWT2_CPB_SCRATCHDISK',
               u'taskID': u'NULL',
               u'logFile': u'%s.job.log.tgz' % job_name}
    else:
        logger.warning('unknown test job type: %s' % config.Pilot.testjobtype)

    if res:
        if not input:
            res['inFiles'] = u'NULL'
            res['GUID'] = u'NULL'
            res['scopeIn'] = u'NULL'
            res['fsize'] = u'NULL'
            res['realDatasetsIn'] = u'NULL'
            res['checksum'] = u'NULL'

        if config.Pilot.testtransfertype == "NULL" or config.Pilot.testtransfertype == 'direct':
            res['transferType'] = config.Pilot.testtransfertype
        else:
            logger.warning('unknown test transfer type: %s (ignored)' % config.Pilot.testtransfertype)

        if config.Pilot.testjobcommand == 'sleep':
            res['transformation'] = 'sleep'
            res['jobPars'] = '1'
            res['inFiles'] = ''
            res['outFiles'] = ''
    return res


def get_job_retrieval_delay(harvester):
    """
    Return the proper delay between job retrieval attempts.
    In Harvester mode, the pilot will look once per second for a job definition file.

    :param harvester: True if Harvester is being used (determined from args.harvester), otherwise False
    :return: sleep (s)
    """

    if harvester:
        delay = 1
    else:
        delay = 60

    return delay


def retrieve(queues, traces, args):
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
    """

    timefloor = infosys.queuedata.timefloor
    starttime = time.time()

    jobnumber = 0  # number of downloaded jobs
    getjob_requests = 0  # number of getjob requests

    print_node_info()

    while not args.graceful_stop.is_set():

        getjob_requests += 1

        if not proceed_with_getjob(timefloor, starttime, jobnumber, getjob_requests, args.harvester, args.verify_proxy):
            args.graceful_stop.set()
            break

        # store time stamp
        time_pre_getjob = time.time()

        # get a job definition from a source (file or server)
        res = get_job_definition(args)
        logger.info('job definition = %s' % str(res))

        if res is None:
            logger.fatal('fatal error in job download loop - cannot continue')
            break

        if not res:
            delay = get_job_retrieval_delay(args.harvester)
            if not args.harvester:
                logger.warning('did not get a job -- sleep %d s and repeat' % delay)
            for i in xrange(delay):
                if args.graceful_stop.is_set():
                    break
                time.sleep(1)
        else:
            # it seems the PanDA server returns StatusCode as an int, but the aCT returns it as a string
            if res['StatusCode'] != '0' and res['StatusCode'] != 0:
                logger.warning('did not get a job -- sleep 60s and repeat -- status: %s' % res['StatusCode'])
                for i in xrange(60):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(1)
            else:
                # update dispatcher data for ES (if necessary)
                res = update_es_dispatcher_data(res)

                # create the job object out of the raw dispatcher job dictionary
                job = create_job(res, args.queue)

                # write time stamps to pilot timing file
                # note: PILOT_POST_GETJOB corresponds to START_TIME in Pilot 1
                add_to_pilot_timing(job.jobid, PILOT_PRE_GETJOB, time_pre_getjob)
                add_to_pilot_timing(job.jobid, PILOT_POST_GETJOB, time.time())

                # add the job definition to the jobs queue and increase the job counter,
                # and wait until the job has finished
                queues.jobs.put(job)

                jobnumber += 1
                while not args.graceful_stop.is_set():
                    if job_has_finished(queues):
                        logger.info('ready for new job')
                        break
                    time.sleep(0.5)


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
    from pilot.info import InfoService, JobInfoProvider

    job = JobData(dispatcher_response)

    jobinfosys = InfoService()
    jobinfosys.init(queue, infosys.confinfo, infosys.extinfo, JobInfoProvider(job))
    job.infosys = jobinfosys
    job.workdir = os.getcwd()

    logger.info('received job: %s (sleep until the job has finished)' % job.jobid)
    logger.info('job details: \n%s' % job)

    # payload environment wants the PandaID to be set, also used below
    os.environ['PandaID'] = job.jobid

    return job


def job_has_finished(queues):
    """
    Has the current payload finished?

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
        log = get_logger(job.jobid)
        log.info("job %s has completed" % job.jobid)
        return True

    #jobid = os.environ.get('PandaID')

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


def queue_monitor(queues, traces, args):
    """
    Monitoring of queues.
    This function monitors queue activity, specifically if a job has finished or failed and then reports to the server.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
    """

    while not args.graceful_stop.is_set():
        # wait a second
        if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility reasons
            break

        job = None

        # check if the job has finished
        try:
            job = queues.finished_jobs.get(block=True, timeout=1)
        except queue.Empty:
            # logger.info("(job still running)")
            pass
        else:
            logger.info("job %s has finished" % job.jobid)
            # make sure that state=finished
            job.state = 'finished'

        # check if the job has failed
        try:
            job = queues.failed_jobs.get(block=True, timeout=1)
        except queue.Empty:
            # logger.info("(job still running)")
            pass
        else:
            logger.info("job %s has failed" % job.jobid)
            # make sure that state=failed
            job.state = 'failed'

            # set job_aborted in case of kill signals
            if args.abort_job.is_set():
                logger.warning('queue monitor detected a set abort_job (due to a kill signal), setting job_aborted')
                args.job_aborted.set()

        # job has not been defined if it's still running
        if job:
            # send final server update
            if job.fileinfo:
                send_state(job, args, job.state, xml=dumps(job.fileinfo))
            else:
                send_state(job, args, job.state)

            # we can now stop monitoring this job, so remove it from the monitored_payloads queue and add it to the
            # completed_jobs queue which will tell retrieve() that it can download another job
            try:
                _job = queues.monitored_payloads.get(block=True, timeout=1)
            except queue.Empty:
                logger.warning('failed to dequeue job: queue is empty (did job fail before job monitor started?)')
            else:
                logger.info('job %s was dequeued from the monitored payloads queue' % _job.jobid)
                # now ready for the next job (or quit)
                queues.completed_jobs.put(_job)


def get_heartbeat_period(debug=False):
    """
    Return the proper heartbeat period, as determined by normal or debug mode.
    In normal mode, the hearbeat period is 30*60 s, while in debug mode it is 5*60 s. Both values are defined in the
    config file.

    :param debug: Boolean, True for debug mode. False otherwise.
    :return: heartbeat period (int).
    """

    return config.Pilot.heartbeat if not debug else config.Pilot.debug_heartbeat


def job_monitor(queues, traces, args):
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
        if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility reasons
            break

        # sleep for a while if stage-in has not completed
        if queues.finished_data_in.empty():
            # check for any abort_job requests
            if args.abort_job.is_set():
                logger.warning('job monitor detected an abort_job request')
            time.sleep(1)
            continue

        # wait a minute
        time.sleep(60)
        try:
            # peek at the jobs in the validated_jobs queue and send the running ones to the heartbeat function
            jobs = queues.monitored_payloads.queue

            if jobs:
                # update the peeking time
                peeking_time = int(time.time())
                for i in range(len(jobs)):
                    log = get_logger(jobs[i].jobid)
                    log.info('monitor loop #%d: job %d:%s is in state \'%s\'' % (n, i, jobs[i].jobid, jobs[i].state))
                    if jobs[i].state == 'finished' or jobs[i].state == 'failed':
                        log.info('aborting job monitoring since job state=%s' % jobs[i].state)
                        break

                    # perform the monitoring tasks
                    exit_code, diagnostics = job_monitor_tasks(jobs[i], mt, args)
                    if exit_code != 0:
                        jobs[i].state = 'failed'
                        jobs[i].piloterrorcodes, jobs[i].piloterrordiags = errors.add_error_code(exit_code)
                        queues.failed_payloads.put(jobs[i])
                        log.info('aborting job monitoring since job state=%s' % jobs[i].state)
                        break

                    # send heartbeat if it is time (note that the heartbeat function might update the job object, e.g.
                    # by turning on debug mode, ie we need to get the heartbeat period in case it has changed)
                    period = get_heartbeat_period(jobs[i].debug)
                    if int(time.time()) - update_time >= period:
                        send_state(jobs[i], args, 'running')
                        update_time = int(time.time())
            else:
                waiting_time = int(time.time()) - peeking_time
                msg = 'no jobs in monitored_payloads queue (waiting for %d s)' % waiting_time
                if waiting_time > 120:
                    abort = True
                    msg += ' - aborting'
                else:
                    abort = False
                if logger:
                    logger.warning(msg)
                else:
                    print(msg)
                if abort:
                    args.graceful_stop.set()
        except PilotException as e:
            msg = 'exception caught: %s' % e
            if logger:
                logger.warning(msg)
            else:
                print(msg, file=sys.stderr)
        else:
            n += 1
