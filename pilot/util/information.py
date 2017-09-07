#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch


# This is a stub implementation of the information component. It retrieves
# sites, storages, and queues from AGIS and tries to locally cache them.
# No cache update is involved, just remove the .cache files.

import collections
import hashlib
import json
import os
import urllib2

from pilot.util.config import config
import logging
logger = logging.getLogger(__name__)


def set_location(args, site=None):
    '''
    Set up all necessary site information.
    Resolve everything from the specified queue name, and fill extra lookup structure.

    If site is specified, return the site and storage information only.
    '''

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
            logger.critical('queue is not explicitly mapped to a single site, found: %s' % [tmp_site['name'] for tmp_site in matching_sites])
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
                                                    if storage['name'] == tmp_storage and storage['state'] == 'ACTIVE'][0]

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
