#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

import logging
import os
import sys
import platform
import urllib2
import urllib
import json

from exception_formatter import caught

logger = logging.getLogger(__name__)
ssl_context = None
user_agent = None


def capath(args=None):
    if args is not None and args['capath'] is not None and os.path.isdir(args['capath']):
        return args['capath']

    path = os.environ.get('X509_CERT_DIR', '/etc/grid-security/certificates')
    if os.path.isdir(path):
        return path

    return None


def cacert_default_location():
    try:
        return '/tmp/x509up_u%s' % str(os.getuid())
    except AttributeError:
        # Wow, not UNIX? Nevermind, skip.
        pass

    return None


def cacert(args=None):
    if args is not None and args['cacert'] is not None and os.path.isfile(args['cacert']):
        return args['cacert']

    path = os.environ.get('X509_USER_PROXY', cacert_default_location())
    if os.path.isfile(path):
        return path

    return None


def setup(args, name='pilot'):
    global ssl_context

    build_user_agent(name)

    try:
        import ssl
        ssl_context = ssl.create_default_context(
            capath=capath(args),
            cafile=cacert(args))
    except Exception as e:
        logger.warn('SSL communication is impossible due to SSL error:')
        caught(e, sys.exc_info(), level=logging.WARNING)
        pass


def build_user_agent(name='pilot'):
    global user_agent

    user_agent = "%s (Python %s; %s %s; rv:alpha)" % (name, sys.version.split(" ")[0], platform.system(), platform.machine())
    logger.debug("User-Agent: " + user_agent)


def request(url, data=None, plain=False):
    req = urllib2.Request(url, urllib.urlencode(data))
    req.add_header('Accept', 'application/json;'
                             'q=0.9,text/html,application/xhtml+xml,application/xml;'
                             'q=0.7,*/*;'
                             'q=0.5')
    req.add_header('User-Agent', user_agent)
    try:
        result = urllib2.urlopen(req, context=ssl_context)

        if plain:
            return result

        return json.loads(result)
    except urllib2.HTTPError as e:
        logger.warn("Server returned error %d:" % e.code)
        logger.warn(e.read())
    except urllib2.URLError as e:
        logger.warn("Could not reach the server %s:" % e.reason)

    return None
