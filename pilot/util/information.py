#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017


# This is a stub implementation of the information component. It retrieves
# sites, storages, and queues from AGIS and tries to locally cache them.
# No cache update is involved, just remove the .cache files.

import collections
import hashlib
import json
import os
import urllib2
from datetime import datetime, timedelta

from pilot.util.config import config
from pilot.util.filehandling import write_json
from pilot.common.exception import FileHandlingFailure

import logging
logger = logging.getLogger(__name__)


def set_location(args, site=None):
    """
    Set up all necessary site information.
    Resolve everything from the specified queue name, and fill extra lookup structure.

    If site is specified, return the site and storage information only.

    :param args:
    :param site:
    :return:
    """

    args.location = collections.namedtuple('location', ['queue', 'site', 'storages', 'queuedata',
                                                        'queue_info', 'site_info', 'storages_info'])

    if site is None:
        # verify that the queue is active
        url = config.Information.queues
        if url == "":
            logger.fatal('AGIS URL for queues not set')
            return False
        all_queues = retrieve_json(url)
        if args.queue not in [queue['name'] for queue in all_queues]:
            logger.critical('specified queue NOT FOUND: %s -- aborting' % args.queue)
            return False
        if not [queue for queue in all_queues if queue['name'] == args.queue and queue['state'] == 'ACTIVE']:
            logger.critical('specified queue is NOT ACTIVE: %s -- aborting' % args.queue)
            return False
        args.location.queue = str(args.queue)
        args.location.queue_info = [queue for queue in all_queues if queue['name'] == args.queue][0]

        # find the associated site
        url = config.Information.sites
        if url == "":
            logger.fatal('AGIS URL for sites not set')
            return False
        all_sites = retrieve_json(url)
        matching_sites = [queue for queue in all_queues if queue['name'] == args.queue]
        if len(matching_sites) != 1:
            logger.critical('queue is not explicitly mapped to a single site, found: %s' %
                            [tmp_site['name'] for tmp_site in matching_sites])
            return False
        else:
            args.location.site = str(matching_sites[0]['site'])
            args.location.site_info = [tmp_site for tmp_site in all_sites if tmp_site['name'] == args.location.site][0]

    else:
        # find the associated site
        url = config.Information.sites
        if url == "":
            logger.fatal('AGIS URL for sites not set')
            return False
        all_sites = retrieve_json(url)
        result = [tmp_site for tmp_site in all_sites if tmp_site['name'] == site]
        if len(result) == 0:
            raise Exception('Specified site not found: %s' % site)
        args.location.site = site
        args.location.site_info = result[0]

    # find all enabled storages at site
    url = config.Information.storages
    if url == "":
        logger.fatal('AGIS URL for storages not set')
        return False
    all_storages = retrieve_json(url)
    args.location.storages = [str(storage['name']) for storage in all_storages
                              if storage['site'] == args.location.site and storage['state'] == 'ACTIVE']
    args.location.storages_info = {}
    for tmp_storage in args.location.storages:
        args.location.storages_info[tmp_storage] = [storage for storage in all_storages
                                                    if storage['name'] == tmp_storage and
                                                    storage['state'] == 'ACTIVE'][0]

    # find the schedconfig queue data
    url = config.Information.schedconfig
    if url == "":
        logger.fatal('URL for schedconfig not set')
        return False
    else:
        # add the queuename to the URL
        if not url.endswith('/'):
            url += '/'
        url += args.location.queue + '.all.json'
    args.location.queuedata = retrieve_json(url)

    # also write the queuedata to disk
    filename = os.path.join(args.mainworkdir, config.Information.queuedata)
    if not write_json(filename, args.location.queuedata):
        logger.warning("failed to write queuedata json to file")
    else:
        logger.info("wrote queuedata to local file %s" % filename)

    logger.info('queue: %s' % args.location.queue)
    logger.info('site: %s' % args.location.site)
    logger.info('storages: %s' % args.location.storages)
    logger.info('queuedata: %s' % args.location.queuedata)

    return True

def retrieve_json(url):
    logger.debug('retrieving: %s' % url)
    j = _read_cache(url)
    if j is not None:
        logger.debug('cached version found: %s' % url)
        return j
    j = json.loads(urllib2.urlopen(url).read())
    logger.debug('caching: %s' % url)
    _write_cache(url, j)
    return j


def _read_cache(url):
    m = hashlib.md5()
    m.update(url)
    if os.path.exists('.cache.%s' % m.hexdigest()):
        with open('.cache.%s' % m.hexdigest(), 'rb') as infile:
            return json.load(infile)


def _write_cache(url, j):
    m = hashlib.md5()
    m.update(url)
    with open('.cache.%s' % m.hexdigest(), 'wb') as outfile:
        json.dump(j, outfile)


def get_parameter(queuedata, field):
    return queuedata[field] if field in queuedata else None


def is_file_expired(fname, cache_time=0):
    """
    Check if file fname older then cache_time seconds from its last_update_time.

    :param fname:
    :param cache_time:
    :return:
    """

    if cache_time:
        lastupdate = get_file_last_update_time(fname)
        return not (lastupdate and datetime.now() - lastupdate < timedelta(seconds=cache_time))

    return True


def get_file_last_update_time(fname):
    """
    Return the last update time of the given file.

    :param fname:
    :return:
    """

    try:
        lastupdate = datetime.fromtimestamp(os.stat(fname).st_mtime)
    except OSError, e:
        lastupdate = None

    return lastupdate


def load_url_data(url, fname=None, cache_time=0, nretry=3, sleeptime=60):
    """
    Download data from url/file resource and optionally save it into cachefile fname,
    The file will not be (re-)loaded again if cache age from last file modification does not exceed "cache_time"
    seconds.

    :param url:
    :param fname:
    :param cache_time:
    :param nretry:
    :param sleeptime:
    :return: data loaded from the url or file content if url passed is a filename
    """

    content = None
    if url and is_file_expired(fname, cache_time):  # load data into temporary cache file
        for trial in range(nretry):
            if content:
                break
            try:
                if os.path.isfile(url):
                    logger.info('[attempt=%s] Loading data from file=%s' % (trial, url))
                    f = open(url, "r")  # python 2.5 .. replace by 'with' statement (min python2.6??)
                    content = f.read()
                    f.close()
                else:
                    logger.info('[attempt=%s] Loading data from url=%s' % (trial, url))
                    content = urllib2.urlopen(url, timeout=20).read()  # python2.6

                if fname:  # save to cache
                    f = open(fname, "w+")
                    f.write(content)
                    f.close()
                    logger.info('Saved data from "%s" resource into file=%s, length=%.1fKb' % \
                                 (url, fname, len(content) / 1024.))
                return content
            except Exception, e:  # ignore errors, try to use old cache if any
                # tolog("Failed to load data from url=%s, error: %s .. trying to use data from cache=%s" % (
                # url, e, fname))
                # will try to use old cache below
                if trial < nretry - 1:
                    logger.info("Will try again after %ss.." % sleeptime)
                    from time import sleep
                    sleep(sleeptime)

    if content is not None:  # just loaded
        return content

    try:
        with open(fname, 'r') as f:
            content = f.read()
    except Exception, e:
        # tolog("loadURLData: Caught exception: %s" % e)
        return None

    return content


def load_schedconfig_data(args, pandaqueues=[], cache_time=60):
    """
    Download the queuedata from various sources (prioritized).
    Try to get data from CVMFS first, then AGIS or from Panda JSON sources (not implemented).
    Note: as of October 2016, agis_schedconfig.json is complete but contains info from all PQ (5 MB)

    :param args:
    :param pandaqueues:
    :param cache_time:
    :return:
    """

    # list of sources to fetch ddmconf data from
    base_dir = args.mainworkdir
    pandaqueues = set(pandaqueues)

    schedcond_sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_schedconf.json',
                                 'nretry': 1,
                                 'fname': os.path.join(base_dir, 'agis_schedconf.cvmfs.json')},
                        'AGIS':  {'url':'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json'
                                        '&preset=schedconf.all&panda_queue=%s' % ','.join(pandaqueues),
                                 'nretry':3,
                                 'fname': os.path.join(base_dir, 'agis_schedconf.agis.%s.json' % \
                                                       ('_'.join(sorted(pandaqueues)) or 'ALL'))},
                        'PANDA' : None
    }

    schedcond_sources_order = ['CVMFS', 'AGIS'] # can be moved into the schedconfig in order to configure workflow in AGIS on fly: TODO

    for key in schedcond_sources_order:
        dat = schedcond_sources.get(key)
        if not dat:
            continue

        content = load_url_data(cache_time=cache_time, **dat)
        if not content:
            continue
        try:
            data = json.loads(content)
        except Exception, e:
            logger.info("!!WARNING: loadSchedConfData(): Failed to parse JSON content from source=%s .. skipped, error=%s" % (dat.get('url'), e))
            data = None

        if data and isinstance(data, dict):
            if 'error' in data:
                logger.info("!!WARNING: loadSchedConfData(): skipped source=%s since response contains error: data=%s" % (dat.get('url'), data))
            else: # valid response
                return data

    return None
