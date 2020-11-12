# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

"""
Base loader class to retrive data from Ext sources (file, url)

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import os
import time
import json
try:
    import urllib.request  # Python 3
    import urllib.error  # Python 3
    import urllib.parse  # Python 3
except Exception:
    import urllib2  # Python 2

from datetime import datetime, timedelta

from pilot.util.auxiliary import is_python3
from pilot.util.timer import timeout
from pilot.util.https import ctx

import logging
logger = logging.getLogger(__name__)


class DataLoader(object):
    """
        Base data loader
    """

    @classmethod
    def is_file_expired(self, fname, cache_time=0):
        """
        Check if file fname is older than cache_time seconds from its last_update_time.

        :param fname: File name.
        :param cache_time: Cache time in seconds.
        :return: Boolean.
        """

        if cache_time:
            lastupdate = self.get_file_last_update_time(fname)
            return not (lastupdate and datetime.now() - lastupdate < timedelta(seconds=cache_time))

        return True

    @classmethod
    def get_file_last_update_time(self, fname):
        """
        Return the last update time of the given file.

        :param fname: File name.
        :return: Last update time in seconds or None if file does not exist.
        """

        try:
            lastupdate = datetime.fromtimestamp(os.stat(fname).st_mtime)
        except Exception:
            lastupdate = None

        return lastupdate

    @classmethod  # noqa: C901
    def load_url_data(self, url, fname=None, cache_time=0, nretry=3, sleep_time=60):
        """
        Download data from url or file resource and optionally save it into cache file fname.
        The file will not be (re-)loaded again if cache age from last file modification does not exceed cache_time
        seconds.

        If url is None then data will be read from cache file fname (if any)

        :param url: Source of data
        :param fname: Cache file name. If given then loaded data will be saved into it.
        :param cache_time: Cache time in seconds.
        :param nretry: Number of retries (default is 3).
        :param sleep_time: Sleep time (default is 60 s) between retry attempts.
        :return: data loaded from the url or file content if url passed is a filename.
        """

        @timeout(seconds=20)
        def _readfile(url):
            if os.path.isfile(url):
                with open(url, "r") as f:
                    content = f.read()
                return content

        content = None
        if url and self.is_file_expired(fname, cache_time):  # load data into temporary cache file
            for trial in range(1, nretry + 1):
                if content:
                    break
                try:
                    native_access = '://' not in url  ## trival check for file access, non accurate.. FIXME later if need
                    if native_access:
                        logger.info('[attempt=%s/%s] loading data from file=%s' % (trial, nretry, url))
                        content = _readfile(url)
                    else:
                        logger.info('[attempt=%s/%s] loading data from url=%s' % (trial, nretry, url))

                        try:
                            req = urllib.request.Request(url)  # Python 3
                        except Exception:
                            req = urllib2.Request(url)  # Python 2

                        req.add_header('User-Agent', ctx.user_agent)

                        try:
                            content = urllib.request.urlopen(req, context=ctx.ssl_context, timeout=20).read()  # Python 3
                        except Exception:
                            content = urllib2.urlopen(req, context=ctx.ssl_context, timeout=20).read()  # Python 2
                    if fname:  # save to cache
                        with open(fname, "w+") as f:
                            if isinstance(content, bytes) and is_python3():  # if-statement will always be needed for python 3
                                content = content.decode("utf-8")  # Python 2/3 - only works for byte streams in python 3
                            f.write(content)  # Python 3, added str (write() argument must be str, not bytes; JSON OK)
                            logger.info('saved data from "%s" resource into file=%s, length=%.1fKb' %
                                        (url, fname, len(content) / 1024.))
                    return content
                except Exception as e:  # ignore errors, try to use old cache if any
                    logger.warning('failed to load data from url=%s, error: %s .. trying to use data from cache=%s' %
                                   (url, e, fname))
                    # will try to use old cache below
                    if trial < nretry:
                        xsleep_time = sleep_time() if callable(sleep_time) else sleep_time
                        logger.info("will try again after %ss.." % xsleep_time)
                        time.sleep(xsleep_time)

        if content is not None:  # just loaded data
            return content

        # read data from old cache fname
        try:
            with open(fname, 'r') as f:
                content = f.read()
        except Exception as e:
            logger.warning("cache file=%s is not available: %s .. skipped" % (fname, e))
            return None

        return content

    @classmethod
    def load_data(self, sources, priority, cache_time=60, parser=None):
        """
        Download data from various sources (prioritized).
        Try to get data from sources according to priority values passed

        Expected format of source entry:
        sources = {'NAME':{'url':"source url", 'nretry':int, 'fname':'cache file (optional)', 'cache_time':int (optional), 'sleep_time':opt}}

        :param sources: Dict of source configuration
        :param priority: Ordered list of source names
        :param cache_time: Default cache time in seconds. Can be overwritten by cache_time value passed in sources dict
        :param parser: Callback function to interpret/validate data which takes read data from source as input. Default is json.loads
        :return: Data loaded and processed by parser callback
        """

        if not priority:  # no priority set ## randomly order if need (FIX ME LATER)
            priority = list(sources.keys())  # Python 3

        for key in priority:
            dat = sources.get(key)
            if not dat:
                continue

            accepted_keys = ['url', 'fname', 'cache_time', 'nretry', 'sleep_time']
            idat = dict([k, dat.get(k)] for k in accepted_keys if k in dat)
            idat.setdefault('cache_time', cache_time)

            content = self.load_url_data(**idat)
            if isinstance(content, bytes) and is_python3():
                content = content.decode("utf-8")
                logger.debug('converted content to utf-8')
            if not content:
                continue
            if dat.get('parser'):
                parser = dat.get('parser')
            if not parser:
                def jsonparser(c):
                    dat = json.loads(c)
                    if dat and isinstance(dat, dict) and 'error' in dat:
                        raise Exception('response contains error, data=%s' % dat)
                    return dat
                parser = jsonparser
            try:
                data = parser(content)
            except Exception as e:
                logger.fatal("failed to parse data from source=%s (resource=%s, cache=%s).. skipped, error=%s" % (dat.get('url'), key, dat.get('fname'), e))
                data = None
            if data:
                return data

        return None


def merge_dict_data(d1, d2, keys=[], common=True, left=True, right=True, rec=False):
    """
        Recursively merge two dict objects
        Merge content of d2 dict into copy of d1
        :param common: if True then do merge keys exist in both dicts
        :param left: if True then preseve keys exist only in d1
        :param right: if True then preserve keys exist only in d2
    """

    ### TODO: verify and configure logic later

    if not(type(d1) == type(d2) and type(d1) is dict):
        return d2

    ret = d1.copy()

    if keys and rec:
        for k in set(keys) & set(d2):
            ret[k] = d2[k]
        return ret

    if common:  # common
        for k in set(d1) & set(d2):
            ret[k] = merge_dict_data(d1[k], d2[k], keys, rec=True)

    if not left:  # left
        for k in set(d1) - set(d2):
            ret.pop(k)

    if right:  # right
        for k in set(d2) - set(d1):
            ret[k] = d2[k]

    return ret
