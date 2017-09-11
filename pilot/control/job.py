#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch

import Queue
import os
import threading
import time
import urllib

from pilot.util import https
from pilot.util.config import config
from pilot.util.disk import disk_usage
from pilot.util.parameters import get_maximum_input_sizes

import logging
logger = logging.getLogger(__name__)


def control(queues, traces, args):

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
    # valid = random.uniform(0, 100)
    # if valid > 99:
    #     logger.warning('%s: job did not validate correctly -- skipping' % job['PandaID'])
    #     job['errno'] = random.randint(0, 100)
    #     job['errmsg'] = 'job failed random validation'
    #     return False
    return True


def send_state(job, args, state, xml=None):

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

    while not args.graceful_stop.is_set():
        try:
            job = queues.jobs.get(block=True, timeout=1)
        except Queue.Empty:
            continue
        log = logger.getChild(str(job['PandaID']))

        traces.pilot['nr_jobs'] += 1

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

    while not args.graceful_stop.is_set():
        try:
            job = queues.validated_jobs.get(block=True, timeout=1)
        except Queue.Empty:
            continue

        queues.data_in.put(job)
        queues.payloads.put(job)


# UNIT TEST MISSING (should return non-zero float, float)
def collect_WN_info():
    """
    Collect node information (cpu and memory).

    :return: mem (float), cpu (float)
    """

    mem = 0.
    cpu = 0.

    try:
        with open("/proc/meminfo", "r") as fd:
            mems = fd.readline()
            while mems:
                if mems.upper().find("MEMTOTAL") != -1:
                    mem = float(mems.split()[1]) / 1024
                    break
                mems = fd.readline()
    except IOError as e:
        logger.warning("failed to read /proc/meminfo: %s" % e)

    try:
        with open("/proc/cpuinfo", "r") as fd:
            lines = fd.readlines()
            for line in lines:
                if not string.find(line, "cpu MHz"):
                    cpu = float(line.split(":")[1])
                    break
    except IOError as e:
        logger.warning("failed to read /proc/cpuinfo: %s" % e)

    return mem, cpu


# UNIT TEST MISSING (should return non-zero int)
def get_disk_space_for_dispatcher(queuedata):
    """

    Return the amound of disk space that should be available for running the job, either what is actually locally
    available or the allowed size determined by the site (value from queuedata). This value is only to be used
    internally by the job dispatcher.

    :param queuedata: (better to have a file based queuedata a la pilot 1 or some singleton memory resident object?)
    :return: disk space that should be available for running the job
    """

    _maxinputsize = get_maximum_input_sizes(queuedata)
    try:
        du = disk_usage(os.path.abspath("."))
        _diskspace = int(du[2]/(1024*1024))  # need to convert from B to MB
    except ValueError, e:
        logger.warning("Failed to extract disk space: %s (will use schedconfig default)" % e)
        _diskspace = _maxinputsize
    else:
        logger.info("Available WN disk space: %d MB" % (_diskspace))

    _diskspace = min(_diskspace, _maxinputsize)
    logger.info("Sending disk space %d MB to dispatcher" % (_diskspace))

    return _diskspace


# UNIT TEST MISSING
def get_dispatcher_dictionary(args):
    """
    Return a dictionary with required fields for the dispatcher getJob operation.

    The dictionary should contain the following fields:

    :returns: dictionary prepared for the dispatcher getJob operation.
    """

    _diskspace = get_disk_space_for_dispatcher(args.location.queuedata)

    data = {
        'siteName': args.location.queue,
        'prodSourceLabel': args.job_label,
        'diskSpace': _diskspace,
        'workingGroup': args.workinggroup,
        'countryGroup': args.countrygroup
    }

#    jNode = {'siteName': env['thisSite'].sitename,
#             'cpu': env['workerNode'].cpu,
#            'mem': env['workerNode'].mem,
#             'diskSpace': _diskSpace,
#             'node': nodename,
#             'computingElement': env['thisSite'].computingElement,
#             'getProxyKey': _getProxyKey,
#             'workingGroup': env['workingGroup']}

    return data


def retrieve(queues, traces, args):
    """ 
    Retrieve a job definition from a source (server or pre-placed local file [not yet implemented]).

    The job definition is a json dictionary that is either present in the launch
    directory (preplaced) or downloaded from a server specified by `args.url`.

    The function retrieves the job definition from the proper source and places
    it in the `queues.jobs` queue.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot and rucio states.
    :param args: arguments (e.g. containing queue name, queuedata dictionary, etc).
    """

    # get the job dispatcher dictionary
    data = get_dispatcher_dictionary(args)

    while not args.graceful_stop.is_set():

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

        if args.url != "":
            url = args.url + ':' + str(args.port) # args.port is always set
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

        if res is None:
            logger.warning('did not get a job -- sleep 60s and repeat')
            for i in xrange(600):
                if args.graceful_stop.is_set():
                    break
                time.sleep(0.1)
        else:
            if res['StatusCode'] != 0:
                logger.warning('did not get a job -- sleep 60s and repeat -- status: %s' % res['StatusCode'])
                for i in xrange(600):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(0.1)
            else:
                logger.info('got job: %s -- sleep 1000s before trying to get another job' % res['PandaID'])
                queues.jobs.put(res)
                for i in xrange(10000):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(0.1)
