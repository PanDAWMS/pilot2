#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017


# This is a stub implementation of the information component. It retrieves
# sites, storages, and queues from AGIS and tries to locally cache them.
# No cache update is involved, just remove the .cache files.

import hashlib
import json
import os
import urllib2

import logging
logger = logging.getLogger(__name__)

url_sites = 'http://atlas-agis-api.cern.ch/request/site/query/list/?json'
url_storages = 'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json'
url_queues = 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json'


def get_sites():
    return retrieve_json(url_sites)


def get_storages():
    return retrieve_json(url_storages)


def get_queues():
    return retrieve_json(url_queues)


def retrieve_json(url):
    logger.debug('retrieving: {0}'.format(url))
    j = _read_cache(url)
    if j is not None:
        logger.debug('cached version found: {0}'.format(url))
        return j
    j = json.loads(urllib2.urlopen(url).read())
    logger.debug('caching: {0}'.format(url))
    _write_cache(url, j)
    return j


def _read_cache(url):
    m = hashlib.md5()
    m.update(url)
    if os.path.exists('.cache.{0}'.format(m.hexdigest())):
        with open('.cache.{0}'.format(m.hexdigest()), 'rb') as infile:
            return json.load(infile)


def _write_cache(url, j):
    m = hashlib.md5()
    m.update(url)
    with open('.cache.{0}'.format(m.hexdigest()), 'wb') as outfile:
        json.dump(j, outfile)
