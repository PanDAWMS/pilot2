"""
Base loader class to retrive data from Ext sources (file, url)

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import os
import time, json, urllib2

from datetime import datetime, timedelta

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


    @classmethod
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

        content = None
        if url and self.is_file_expired(fname, cache_time):  # load data into temporary cache file
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
                        content = urllib2.urlopen(url, timeout=20).read()

                    if fname:  # save to cache
                        with open(fname, "w+") as f:
                            f.write(content)
                            logger.info('saved data from "%s" resource into file=%s, length=%.1fKb' %
                                        (url, fname, len(content) / 1024.))
                    return content
                except Exception, e:  # ignore errors, try to use old cache if any
                    logger.warning('failed to load data from url=%s, error: %s .. trying to use data from cache=%s' %
                                   (url, e, fname))
                    # will try to use old cache below
                    if trial < nretry - 1:
                        xsleep_time = sleep_time() if callable(sleep_time) else sleep_time
                        logger.info("will try again after %ss.." % xsleep_time)
                        time.sleep(xsleep_time)

        if content is not None:  # just loaded data
            return content

        # read data from old cache fname
        try:
            with open(fname, 'r') as f:
                content = f.read()
        except Exception, e:
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

        if not priority: # no priority set ## rundomly order if need (FIX ME LATER)
            priority = sources.keys()

        for key in priority:
            dat = sources.get(key)
            if not dat:
                continue

            accepted_keys = ['url', 'fname', 'cache_time', 'nretry', 'sleep_time']
            idat = dict([k, dat.get(k)] for k in accepted_keys if k in dat)
            idat.setdefault('cache_time', cache_time)

            content = self.load_url_data(**idat)
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
            except Exception, e:
                logger.fatal("failed to parse data from source=%s .. skipped, error=%s" % (dat.get('url'), e))
                data = None
            if data:
                return data

        return None


def merge_dict_data(d1, d2, keys=[], common=True, left=True, right=True, rec=False):
    """
        Recursively merge two dict objects
    """

    ### TODO: verify and configure logic later

    if not(type(d1) == type(d2) and type(d1) is dict):
        return d2

    ret = d1.copy()

    if keys and rec:
        for k in set(keys) & set(d2):
            ret[k] = d2[k]
        return ret

    if common: # common
        for k in set(d1) & set(d2):
            ret[k] = merge_dict_data(d1[k], d2[k], keys, rec=True)

    if not left: # left
        for k in set(d1) - set(d2):
            ret.pop(k)

    if right: # right
        for k in set(d2) - set(d1):
            ret[k] = d2[k]

    return ret
