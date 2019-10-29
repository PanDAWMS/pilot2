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

## THIS FILE CAN BE COMPLETELY REMOVED

import collections
import hashlib
import json
import os
try:
    import urllib.request  # Python 3
    import urllib.error  # Python 3
    import urllib.parse  # Python 3
except Exception:
    import urllib2  # Python 3
from datetime import datetime, timedelta

from pilot.util.config import config
from pilot.util.filehandling import write_json, read_json
# from pilot.common.exception import FileHandlingFailure
from pilot.util.timer import timeout

import logging
logger = logging.getLogger(__name__)


def set_location(args, site=None):  ## TO BE DEPRECATED -- not used
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
    args.location.queuedata = get_schedconfig_queuedata(args.location.queue)

    logger.info('queue: %s' % args.location.queue)
    logger.info('site: %s' % args.location.site)
    logger.info('storages: %s' % args.location.storages)
    logger.info('queuedata: %s' % args.location.queuedata)

    return True


def get_schedconfig_queuedata(queue):
    """
    Return and store the schedconfig queuedata.

    :param queue: PanDA queue name (e.g. BNL_PROD_MCORE)
    :return: schedconfig queuedata json dictionary
    """

    # read it locally if the queuedata file already exists
    filename = os.path.join(os.environ.get('PILOT_HOME'), config.Information.queuedata)
    if os.path.exists(filename):
        queuedata = read_json(filename)
        return queuedata

    url = config.Information.schedconfig
    if url == "":
        logger.fatal('URL for schedconfig not set')
        return False
    else:
        # add the queuename to the URL
        if not url.endswith('/'):
            url += '/'
        url += queue + '.all.json'
    queuedata = retrieve_json(url)

    # also write the queuedata to disk
    if not write_json(filename, queuedata):
        logger.warning("failed to write queuedata json to file")
    else:
        logger.info("wrote queuedata to local file %s" % filename)

    return queuedata


@timeout(seconds=120)
def retrieve_json(url):
    """
    Retrieve JSON from an URL or from a local cache.
    Store the JSON in a local cache if it doesn't exist already.

    :param url:
    :return: JSON dictionary.
    """

    logger.info('retrieving: %s' % url)
    j = _read_cache(url)
    if j is not None:
        logger.debug('cached version found: %s' % url)
        return j
    try:
        j = json.loads(urllib.request.urlopen(url).read())  # Python 3
    except Exception:
        j = json.loads(urllib2.urlopen(url).read())  # Python 2
    logger.info('caching: %s' % url)
    _write_cache(url, j)

    return j


def _read_cache(url):
    """
    Read the local cache file that contains the previously downloaded JSON from URL.

    :param url:
    :return: JSON dictionary.
    """

    j = None
    m = hashlib.md5()
    m.update(url)
    if os.path.exists('.cache.%s' % m.hexdigest()):
        with open('.cache.%s' % m.hexdigest(), 'rb') as infile:
            j = json.load(infile)

    return j


def _write_cache(url, j):
    """
    Write the JSON from URL into a local cache.

    :param url:
    :param j: JSON dictionary.
    :return:
    """

    m = hashlib.md5()
    m.update(url)
    with open('.cache.%s' % m.hexdigest(), 'wb') as outfile:
        json.dump(j, outfile)


def is_file_expired(fname, cache_time=0):
    """
    Check if file fname is older than cache_time seconds from its last_update_time.

    :param fname: File name.
    :param cache_time: Cache time in seconds.
    :return: Boolean.
    """

    if cache_time:
        lastupdate = get_file_last_update_time(fname)
        return not (lastupdate and datetime.now() - lastupdate < timedelta(seconds=cache_time))

    return True


def get_file_last_update_time(fname):
    """
    Return the last update time of the given file.

    :param fname: File name.
    :return: Last update time in seconds.
    """

    try:
        lastupdate = datetime.fromtimestamp(os.stat(fname).st_mtime)
    except Exception:
        lastupdate = None

    return lastupdate


def load_url_data(url, fname=None, cache_time=0, nretry=3, sleeptime=60):
    """
    Download data from url/file resource and optionally save it into cache file fname.
    The file will not be (re-)loaded again if cache age from last file modification does not exceed cache_time
    seconds.

    :param url:
    :param fname: File name.
    :param cache_time: Cache time in seconds.
    :param nretry: Number of retries (default is 3).
    :param sleeptime: Sleep time (default is 60 s) between retry attempts.
    :return: data loaded from the url or file content if url passed is a filename.
    """

    content = None
    if url and is_file_expired(fname, cache_time):  # load data into temporary cache file
        for trial in range(nretry):
            if content:
                break
            try:
                if os.path.isfile(url):
                    logger.info('[attempt=%s] loading data from file=%s' % (trial, url))
                    with open(url, "r") as f:
                        content = f.read()
                else:
                    logger.info('[attempt=%s] loading data from url=%s' % (trial, url))
                    try:
                        content = urllib.request.urlopen(url, timeout=20).read()  # Python 3
                    except Exception:
                        content = urllib2.urlopen(url, timeout=20).read()  # Python 2

                if fname:  # save to cache
                    with open(fname, "w+") as f:
                        f.write(content)
                        logger.info('saved data from "%s" resource into file=%s, length=%.1fKb' %
                                    (url, fname, len(content) / 1024.))
                return content
            except Exception as e:  # ignore errors, try to use old cache if any
                logger.warning('failed to load data from url=%s, error: %s .. trying to use data from cache=%s' %
                               (url, e, fname))
                # will try to use old cache below
                if trial < nretry - 1:
                    logger.info(" -- DEPRECATED-- will try again after %ss.." % sleeptime)
                    from time import sleep
                    sleep(sleeptime)

    if content is not None:  # just loaded
        return content

    try:
        with open(fname, 'r') as f:
            content = f.read()
    except Exception as e:
        logger.warning("%s (will try different source)" % e)
        return None

    return content


def load_ddm_conf_data(ddmendpoints=[], cache_time=60):
    """
    Load DDM configuration data from a source.
    Valid sources: CVMFS, AGIS, LOCAL.

    :param ddmendpoints:
    :param cache_time: Cache time in seconds.
    :return:
    """

    # list of sources to fetch ddmconf data from
    ddmconf_sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json',
                                 'nretry': 1,
                                 'fname': os.path.join(os.environ.get('PILOT_HOME', '.'), 'agis_ddmendpoints.cvmfs.json')},
                       'AGIS': {'url': 'http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&'
                                       'state=ACTIVE&preset=dict&ddmendpoint=%s' % ','.join(ddmendpoints),
                                       'nretry': 3,
                                       'fname': os.path.join(os.environ.get('PILOT_HOME', '.'), 'agis_ddmendpoints.agis.%s.json' %
                                                             ('_'.join(sorted(ddmendpoints)) or 'ALL'))},

                       'LOCAL': {'url': None,
                                 'nretry': 1,
                                 'fname': os.path.join(os.environ.get('PILOT_HOME', '.'), 'agis_ddmendpoints.json')},
                       'PANDA': None
                       }

    ddmconf_sources_order = ['LOCAL', 'CVMFS', 'AGIS']

    for key in ddmconf_sources_order:
        logger.info("loading DDMConfData from source %s" % key)
        dat = ddmconf_sources.get(key)
        if not dat:
            continue

        content = load_url_data(cache_time=cache_time, **dat)
        if not content:
            continue
        try:
            data = json.loads(content)
        except Exception as e:
            logger.fatal("failed to parse JSON content from source=%s .. skipped, error=%s" % (dat.get('url'), e))
            data = None

        if data:
            return data

    return None


def resolve_ddm_conf(ddmendpoints):
    """
    Resolve the DDM configuration.

    :return: DDM configuration data from a source (JSON dictionary).
    """

    return load_ddm_conf_data(ddmendpoints, cache_time=6000) or {}


def load_schedconfig_data(pandaqueues=[], cache_time=60):
    """
    Download the queuedata from various sources (prioritized).
    Try to get data from CVMFS first, then AGIS or from Panda JSON sources (not implemented).
    Note: as of October 2016, agis_schedconfig.json is complete but contains info from all PQ (5 MB)

    :param pandaqueues:
    :param cache_time: Cache time in seconds.
    :return:
    """

    # list of sources to fetch ddmconf data from
    pandaqueues = set(pandaqueues)

    schedcond_sources = {'CVMFS': {'url': '/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_schedconf.json',
                                   'nretry': 1,
                                   'fname': os.path.join(os.environ.get('PILOT_HOME', '.'), 'agis_schedconf.cvmfs.json')},
                         'AGIS': {'url': 'http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json'
                                         '&preset=schedconf.all&panda_queue=%s' % ','.join(pandaqueues),
                                  'nretry': 3,
                                  'fname': os.path.join(os.environ.get('PILOT_HOME', '.'), 'agis_schedconf.agis.%s.json' %
                                                        ('_'.join(sorted(pandaqueues)) or 'ALL'))},
                         'LOCAL': {'url': None,
                                   'nretry': 1,
                                   'fname': os.path.join(os.environ.get('PILOT_HOME', '.'), 'agis_schedconf.json')},
                         'PANDA': None}

    schedcond_sources_order = ['LOCAL', 'CVMFS', 'AGIS']

    for key in schedcond_sources_order:
        dat = schedcond_sources.get(key)
        if not dat:
            continue

        content = None
        if key == 'LOCAL':
            if os.path.exists(dat['fname']):
                with open(dat['fname']) as f:
                    content = f.read()
        else:
            content = load_url_data(cache_time=cache_time, **dat)
        if not content:
            continue
        try:
            data = json.loads(content)
        except Exception as e:
            logger.fatal("failed to parse JSON content from source=%s .. skipped, error=%s" % (dat.get('url'), e))
            data = None
        if data:
            if 'error' in data:
                logger.warning("skipped source=%s since response contains error: data=%s" % (dat.get('url'), data))
            else:
                return data

    return None


def resolve_panda_protocols(pandaqueues, activity):
    """
    Resolve PanDA protocols. WARNING: deprecated? aprotocols is always {} (in AGIS).

    Resolve (SE endpoint, path, copytool, copyprefix) protocol entry for requested ddmendpoint by given pilot activity
    ("pr" means pilot_read, "pw" for pilot_write).
    Return the list of possible protocols ordered by priority.

    :param pandaqueues: list of panda queues
    :param activity: activity (string)
    :return: dict('ddmendpoint_name':[(SE_1, path2, copytool, copyprefix), ).
    """

    if not pandaqueues:
        return {}

    schedconf = load_schedconfig_data(pandaqueues, cache_time=6000) or {}

    ret = {}
    for pandaqueue in set(pandaqueues):
        qdata = schedconf.get(pandaqueue, {})
        protocols = qdata.get('aprotocols', {}).get(activity, [])
        copytools = qdata.get('copytools', {})
        for p in protocols:
            p.setdefault('copysetup', copytools.get(p.get('copytool'), {}).get('setup'))
        ret.setdefault(pandaqueue, protocols)

    return ret


def resolve_panda_copytools(pandaqueues, activity, defval=[]):
    """
    Resolve supported copytools by given pandaqueues
    Check first settings for requested activity (pr, pw, pl, pls), then defval values,
    if not set then return copytools explicitly defined for all activities (not restricted to specific activity).
    Return ordered list of accepted copytools.

    :param pandaqueues:
    :param activity: activity of prioritized list of activities to resolve data.
    :param defval: default copytools values which will be used if no copytools defined for requested activity.
    :return: dict('pandaqueue':[(copytool, {settings}), ('copytool_name', {'setup':''}), ]).
    """

    r = load_schedconfig_data(pandaqueues, cache_time=6000) or {}

    ret = {}
    for pandaqueue in set(pandaqueues):
        copytools = r.get(pandaqueue, {}).get('copytools', {})
        cptools = []
        acopytools = None
        for a in activity:
            acopytools = r.get(pandaqueue, {}).get('acopytools', {}).get(a, [])
            if acopytools:
                break
        if acopytools:
            cptools = [(cp, copytools[cp]) for cp in acopytools if cp in copytools]
        elif defval:
            cptools = defval[:]
        else:
            explicit_copytools = set()
            try:
                for v in r.get(pandaqueue, {}).get('acopytools', {}).values():  # Python 3
                    explicit_copytools.update(v or [])
            except Exception:
                for v in r.get(pandaqueue, {}).get('acopytools', {}).itervalues():  # Python 2
                    explicit_copytools.update(v or [])

            try:
                cptools = [(cp, v) for cp, v in copytools.items() if cp not in explicit_copytools]  # Python 3
            except Exception:
                cptools = [(cp, v) for cp, v in copytools.iteritems() if cp not in explicit_copytools]  # Python 2

        ret.setdefault(pandaqueue, cptools)

    return ret


def resolve_panda_os_ddms(pandaqueues):
    """
    Resolve OS ddmendpoints associated with requested pandaqueues.

    :param pandaqueues:
    :return: list of accepted ddmendpoints.
    """

    r = load_schedconfig_data(pandaqueues, cache_time=6000) or {}

    ret = {}
    for pandaqueue in set(pandaqueues):
        ret[pandaqueue] = r.get(pandaqueue, {}).get('ddmendpoints', [])

    return ret


def resolve_panda_associated_storages_for_activity(pandaqueue, activity):
    """
    Resolve DDM storages associated to requested pandaqueue for given activity

    :param pandaqueue: panda queues (string).
    :param activity: activity (string). E.g. 'pr'.
    :return:
    """

    return resolve_panda_associated_storages([pandaqueue]).get(pandaqueue, {})[activity]


def resolve_panda_associated_storages(pandaqueues):
    """
    Resolve DDM storages associated to requested pandaqueues

    :param pandaqueues: panda queues (list).
    :return: list of accepted ddmendpoints.

    :return:
    """

    r = load_schedconfig_data(pandaqueues, cache_time=6000) or {}

    ret = {}
    for pandaqueue in set(pandaqueues):
        ret[pandaqueue] = r.get(pandaqueue, {}).get('astorages', {})

    return ret


def resolve_items(pandaqueues, itemname):
    """
    Resolve DDM storages associated with requested pandaqueues.

    :param pandaqueues:
    :param itemname:
    :return: list of accepted ddmendpoints.
    """

    r = load_schedconfig_data(pandaqueues, cache_time=6000) or {}

    ret = {}
    for pandaqueue in set(pandaqueues):
        ret[pandaqueue] = r.get(pandaqueue, {}).get(itemname, {})

    return ret
