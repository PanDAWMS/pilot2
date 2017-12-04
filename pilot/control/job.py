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
import urllib

from pilot.util import https
from pilot.util.config import config
from pilot.util.workernode import get_disk_space_for_dispatcher, collect_workernode_info, get_node_name
from pilot.util.proxy import get_distinguished_name
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
    log.debug('set job state=%s' % state)

    data = {'jobId': job['PandaID'],
            'state': state}

    if xml is not None:
        data['xml'] = urllib.quote_plus(xml)

    try:
        # cmd = args.url + ':' + str(args.port) + 'server/panda/updateJob'
        # if https.request(cmd, data=data) is not None:

        if https.request('{pandaserver}/server/panda/updateJob'.format(pandaserver=config.Pilot.pandaserver),
                         data=data) is not None:

            log.info('confirmed job state=%s' % state)
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

    _diskspace = get_disk_space_for_dispatcher(args.location.queuedata)
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


def retrieve(queues, traces, args):
    """
    Retrieve a job definition from a source (server or pre-placed local file [not yet implemented]).

    The job definition is a json dictionary that is either present in the launch
    directory (preplaced) or downloaded from a server specified by `args.url`.

    The function retrieves the job definition from the proper source and places
    it in the `queues.jobs` queue.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: arguments (e.g. containing queue name, queuedata dictionary, etc).
    """

    # get the job dispatcher dictionary
    data = get_dispatcher_dictionary(args)

    timefloor = get_timefloor()
    starttime = time.time()

    jobnumber = 0
    getjob_requests = 0
    while not args.graceful_stop.is_set():

        currenttime = time.time()
        if timefloor == 0 and jobnumber > 0:
            logger.warning("since timefloor is set to 0, pilot was only allowed to run one job")
            args.graceful_stop.set()
            break

        if currenttime - starttime > timefloor:
            logger.warning("the pilot has run out of time (timefloor=%d has been passed)" % timefloor)
            args.graceful_stop.set()
            break

        if jobnumber > 0:
            logger.info('since timefloor=%d s and only %d s has passed since launch, pilot can run another job' %
                        (timefloor, currenttime - starttime))

        # getjobmaxtime = 60*5 # to be read from configuration file
        # logger.debug('pilot will attempt job downloads for a maximum of %d seconds' % getjobmaxtime)

        # first check if a job definition exists locally
        # ..

        # logger.debug('trying to fetch job from %s' % args.url)

        # no local job definition, download from server
        # cmd = args.url + ':' + str(args.port) + '/server/panda/getJob'
        # logger.debug('executing command: %s' % cmd)
        # logger.debug('data=%s'%str(data))
        # res = https.request(cmd, data=data)

        getjob_requests += 1
        if getjob_requests > int(config.Pilot.maximum_getjob_requests):
            logger.warning('reached maximum number of getjob requests (%s) -- will abort pilot' %
                           config.Pilot.maximum_getjob_requests)
            args.graceful_stop.set()
            break

        if args.url != "":
            url = args.url + ':' + str(args.port)  # args.port is always set
        else:
            url = config.Pilot.pandaserver
            if url == "":
                logger.fatal('PanDA server url not set (either as pilot option or in config file)')
                break

        if not url.startswith("https://"):
            url = 'https://' + url
            logger.warning('detected missing protocol in server url (added)')

        cmd = '{pandaserver}/server/panda/getJob'.format(pandaserver=url)
        logger.info('executing server command: %s' % cmd)
        res = https.request(cmd, data=data)
        logger.info("job = %s" % str(res))
        if res is None:
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
                queues.jobs.put(res)
                jobnumber += 1
                while not args.graceful_stop.is_set():
                    if job_has_finished(queues) or args.graceful_stop.is_set():
                        break
                    time.sleep(0.5)


def job_has_finished(queues):
    """
    Has the current payload finished?
    :param queues:
    :return: True is the payload has finished
    """

    status = False
    try:
        finishedjob = queues.finished_jobs.get(block=True, timeout=1)
    except Queue.Empty:
        logger.info("(job still running)")
    else:
        logger.info("job %d has finished" % finishedjob['PandaID'])
        status = True

    return status
