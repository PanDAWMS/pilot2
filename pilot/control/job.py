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

import Queue
import os
import threading
import time
from json import dumps

from pilot.util import https
from pilot.util.config import config
from pilot.util.workernode import get_disk_space, collect_workernode_info, get_node_name
from pilot.util.proxy import get_distinguished_name
from pilot.util.auxiliary import time_stamp, get_batchsystem_jobid, get_job_scheduler_id, get_pilot_id
from pilot.util.information import get_timefloor

import logging
logger = logging.getLogger(__name__)


def control(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    threads = [threading.Thread(target=validate,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=retrieve,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=create_data_payload,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args}),
               threading.Thread(target=job_monitor,
                                kwargs={'queues': queues,
                                        'traces': traces,
                                        'args': args})]

    [t.start() for t in threads]


def _validate_job(job):
    """
    (add description)

    :param job:
    :return:
    """
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('%s: job did not validate correctly -- skipping' % job['PandaID'])
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'job failed random validation'
    #     return False
    return True


def send_state(job, args, state, xml=None):
    """
    Update the server (heartbeat message).

    :param job:
    :param args:
    :param state:
    :param xml:
    :return:
    """

    log = logger.getChild(str(job['PandaID']))
    if state == 'finished':
        log.info('job %d has finished - sending final server update' % job['PandaID'])
    else:
        log.debug('set job state=%s' % state)

    # report the batch system job id, if available
    batchsystem_type, batchsystem_id = get_batchsystem_jobid()

    data = {'jobId': job['PandaID'],
            'state': state,
            'timestamp': time_stamp(),
            'siteName': args.site,
            'node': get_node_name()}

    # error codes
    pilotErrorCode = job.get('pilotErrorCode', 0)
    pilotErrorCodes = job.get('pilotErrorCodes', [])
    if pilotErrorCodes != []:
        log.warning('pilotErrorCodes = %s (will report primary/first error code)' % str(pilotErrorCodes))
        data['pilotErrorCode'] = pilotErrorCodes[0]
    else:
        data['pilotErrorCode'] = pilotErrorCode

    pilotErrorDiag = job.get('pilotErrorDiag', 0)
    pilotErrorDiags = job.get('pilotErrorDiags', [])
    if pilotErrorDiags != []:
        log.warning('pilotErrorDiags = %s (will report primary/first error diag)' % str(pilotErrorDiags))
        data['pilotErrorDiag'] = pilotErrorDiags[0]
    else:
        data['pilotErrorDiag'] = pilotErrorDiag

    data['transExitCode'] = job.get('transExitCode', 0)
    data['exeErrorCode'] = job.get('exeErrorCode', 0)
    data['exeErrorDiag'] = job.get('exeErrorDiag', '')

    schedulerid = get_job_scheduler_id()
    if schedulerid:
        data['schedulerID'] = schedulerid

    pilotid = get_pilot_id()
    if pilotid:
        use_newmover_tag = 'DEPRECATED'
        version_tag = args.version_tag
        pilot_version = os.environ.get('PILOT_VERSION')

        if batchsystem_type:
            data['pilotID'] = "%s|%s|%s|%s|%s" % (pilotid, use_newmover_tag, batchsystem_type, version_tag, pilot_version)
            data['batchID'] = batchsystem_id,
        else:
            data['pilotID'] = "%s|%s|%s|%s" % (pilotid, use_newmover_tag, version_tag, pilot_version)

    if xml is not None:
        data['xml'] = xml

    try:
        # cmd = args.url + ':' + str(args.port) + 'server/panda/updateJob'
        # if https.request(cmd, data=data) is not None:

        if https.request('{pandaserver}/server/panda/updateJob'.format(pandaserver=config.Pilot.pandaserver),
                         data=data) is not None:

            log.info('server updateJob request completed for job %s' % job['PandaID'])
            return True
    except Exception as e:
        log.warning('while setting job state, Exception caught: %s' % str(e.message))
        pass

    log.warning('set job state=%s failed' % state)
    return False


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
        except Queue.Empty:
            continue
        log = logger.getChild(str(job['PandaID']))

        traces.pilot['nr_jobs'] += 1

        # set the environmental variable for the task id
        os.environ['PanDA_TaskID'] = job['taskID']
        logger.info('processing PanDA job %s from task %s' % (job['PandaID'], job['taskID']))

        if _validate_job(job):

            log.debug('creating job working directory')
            job_dir = os.path.join(args.mainworkdir, 'PanDA_Pilot-%s' % job['PandaID'])
            try:
                os.mkdir(job_dir)
                job['working_dir'] = job_dir
            except Exception as e:
                log.debug('cannot create working directory: %s' % str(e))
                queues.failed_jobs.put(job)
                break

            log.debug('symlinking pilot log')
            try:
                os.symlink('../pilotlog.txt', os.path.join(job_dir, 'pilotlog.txt'))
            except Exception as e:
                log.debug('cannot symlink pilot log: %s' % str(e))
                queues.failed_jobs.put(job)
                break

            queues.validated_jobs.put(job)
        else:
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
        except Queue.Empty:
            continue

        queues.data_in.put(job)
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

    _diskspace = get_disk_space(args.location.queuedata)
    _mem, _cpu = collect_workernode_info()
    _nodename = get_node_name()

    data = {
        'siteName': args.resource,
        'computingElement': args.location.queue,
        'prodSourceLabel': args.job_label,
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

    return data


def proceed_with_getjob(timefloor, starttime, jobnumber, getjob_requests):
    """
    Can we proceed with getjob?
    We may not proceed if we have run out of time (timefloor limit) or if we have already proceed enough jobs

    :param timefloor: timefloor limit (s)
    :param starttime: start time of retrieve() (s)
    :param jobnumber: number of downloaded jobs
    :param getjob_requests: number of getjob requests
    :return: Boolean based on the input parameters
    """

    currenttime = time.time()

    if getjob_requests > int(config.Pilot.maximum_getjob_requests):
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

    return True


def getjob_server_command(url, port):
    """
    Prepare the getJob server command.

    :param url: PanDA server URL (string)
    :param port: PanDA server port
    :return: full server command (URL string)
    """

    cmd = ""
    if url != "":
        url = url + ':%s' % port  # port is always set
    else:
        url = config.Pilot.pandaserver
    if url == "":
        logger.fatal('PanDA server url not set (either as pilot option or in config file)')
    elif not url.startswith("https://"):
        url = 'https://' + url
        logger.warning('detected missing protocol in server url (added)')

        cmd = '{pandaserver}/server/panda/getJob'.format(pandaserver=url)

    return cmd


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


def get_job_definition_from_file(path):
    """
    Get a job definition from a pre-placed file.

    :param path: path to job definition file.
    :return: job definition dictionary.
    """

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
            res = {}
            for d in datalist:
                res[d[0]] = d[1]

    return res


def get_job_definition_from_server(args):
    """
    Get a job definition from a server.

    :param args:
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
    :param args:
    :return: job definition dictionary.
    """

    res = {}

    path = os.path.join(environ['PILOT_HOME'], config.Pilot.pandajobdata)
    if os.path.exists(path):
        logger.info('will read job definition from file %s' % path)
        res = get_job_definition_from_file()
    else:
        logger.info('will download job definition from server')
        res = get_job_definition_from_server(args)

    return res


def retrieve(queues, traces, args):
    """
    Retrieve all jobs from a source.

    The job definition is a json dictionary that is either present in the launch
    directory (preplaced) or downloaded from a server specified by `args.url`.

    The function retrieves the job definition from the proper source and places
    it in the `queues.jobs` queue.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: arguments (e.g. containing queue name, queuedata dictionary, etc).
    """

    timefloor = get_timefloor()
    starttime = time.time()

    jobnumber = 0  # number of downloaded jobs
    getjob_requests = 0  # number of getjob requests
    while not args.graceful_stop.is_set():

        getjob_requests += 1

        if not proceed_with_getjob(timefloor, starttime, jobnumber, getjob_requests):
            args.graceful_stop.set()
            break

        # get a job definition from a source (file or server)
        res = get_job_definition(args)

        if res is None:
            logger.fatal('fatal error in job download loop - cannot continue')
            break

        if res == {}:
            logger.warning('did not get a job -- sleep 60s and repeat')
            for i in xrange(60):
                if args.graceful_stop.is_set():
                    break
                time.sleep(1)
        else:
            if res['StatusCode'] != 0:
                logger.warning('did not get a job -- sleep 60s and repeat -- status: %s' % res['StatusCode'])
                for i in xrange(60):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(1)
            else:
                logger.info('received job: %s (sleep until the job has finished)' % res['PandaID'])

                # payload environment wants the PandaID to be set, also used below
                os.environ['PandaID'] = str(res['PandaID'])

                # update dispatcher data for ES (if necessary)
                res = update_es_dispatcher_data(res)

                # add the job definition to the jobs queue and increase the job counter,
                # and wait until the job has finished
                queues.jobs.put(res)
                jobnumber += 1
                if args.graceful_stop.is_set():
                    logger.info('graceful stop is currently set')
                else:
                    logger.info('graceful stop is currently not set')
                while not args.graceful_stop.is_set():
                    if job_has_finished(queues):
                        logger.info('graceful stop has been set')
                        break
                    time.sleep(0.5)


def job_has_finished(queues):
    """
    Has the current payload finished?

    :param queues:
    :return: True is the payload has finished or failed
    """

    try:
        panda_id = int(os.environ.get('PandaID', 0))
    except TypeError:
        panda_id = 0

    # is there anything in the finished_jobs queue?
    finished_queue_snapshot = list(queues.finished_jobs.queue)
    peek = [s_job for s_job in finished_queue_snapshot if panda_id == s_job['PandaID']]
    if len(peek) != 0:
        logger.info("job %d has completed (finished)" % panda_id)
        return True

    # is there anything in the failed_jobs queue?
    failed_queue_snapshot = list(queues.failed_jobs.queue)
    peek = [s_job for s_job in failed_queue_snapshot if panda_id == s_job['PandaID']]
    if len(peek) != 0:
        logger.info("job %d has completed (failed)" % panda_id)
        return True

    return False


def job_monitor(queues, traces, args):
    """
    Monitoring of job parameters.
    This function monitors certain job parameters, such as job looping. It also monitors queue activity, specifically
    if a job has finished or failed and then reports to the server.

    :param queues:
    :param traces:
    :param args:
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
        except Queue.Empty:
            # logger.info("(job still running)")
            pass
        else:
            logger.info("job %d has finished" % job['PandaID'])

        # check if the job has failed
        try:
            job = queues.failed_jobs.get(block=True, timeout=1)
        except Queue.Empty:
            # logger.info("(job still running)")
            pass
        else:
            logger.info("job %d has failed" % job['PandaID'])

        # job has not been defined if it's still running
        if job:
            # send final server update
            if job['state'] == "finished":
                send_state(job, args, 'finished', xml=dumps(job['fileinfodict']))
            else:
                send_state(job, args, 'failed')

            # now ready for the next job (or quit)
