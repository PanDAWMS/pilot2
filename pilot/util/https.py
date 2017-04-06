#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Mario Lassnig, mario.lassnig@cern.ch, 2017

import collections
import commands
import json
import os
import platform
import ssl
import sys
import urllib
import urllib2
import pipes

import logging
logger = logging.getLogger(__name__)

_ctx = collections.namedtuple('_ctx', 'ssl_context user_agent capath cacert')


def capath(args=None):
    if args is not None and args.capath is not None and os.path.isdir(args.capath):
        return args.capath

    path = os.environ.get('X509_CERT_DIR', '/etc/grid-security/certificates')
    if os.path.isdir(path):
        return path

    return None


def cacert_default_location():
    try:
        return '/tmp/x509up_u%s' % str(os.getuid())
    except AttributeError:
        logger.warn('No UID available? System not POSIX-compatible... trying to continue')
        pass

    return None


def cacert(args=None):
    if args is not None and args.cacert is not None and os.path.isfile(args.cacert):
        return args.cacert

    path = os.environ.get('X509_USER_PROXY', cacert_default_location())
    if os.path.isfile(path):
        return path

    return None


def https_setup(args, version):
    _ctx.user_agent = 'pilot/%s (Python %s; %s %s)' % (version,
                                                       sys.version.split()[0],
                                                       platform.system(),
                                                       platform.machine())
    logger.debug('User-Agent: %s' % _ctx.user_agent)

    _ctx.capath = capath(args)
    _ctx.cacert = capath(args)

    if sys.version_info < (2, 7, 9):
        logger.warn('Python version <2.7.9 lacks SSL contexts -- falling back to curl')
        _ctx.ssl_context = None
    else:
        try:
            _ctx.ssl_context = ssl.create_default_context(capath=_ctx.capath,
                                                          cafile=_ctx.cacert)
        except Exception as e:
            logger.warn('SSL communication is impossible due to SSL error: %s -- falling back to curl' % str(e))
            _ctx.ssl_context = None

    return True


def request(url, data=None, plain=False):

    # _ctx.ssl_context = None  # no time to deal with this now

    if _ctx.ssl_context is None:
        req = 'curl -sS --compressed --connect-timeout %s --max-time %s '\
              '--capath %s --cert %s --cacert %s --key %s '\
              '-H %s %s %s' % (1, 3,
                               pipes.quote(_ctx.capath), pipes.quote(_ctx.cacert), pipes.quote(_ctx.cacert), pipes.quote(_ctx.cacert),
                               pipes.quote('User-Agent: %s' % _ctx.user_agent),
                               "-H " + pipes.quote('Accept: application/json') if not plain else '',
                               pipes.quote(url + '?' + urllib.urlencode(data) if data else ''))
        logger.debug('request: %s' % req)
        status, output = commands.getstatusoutput(req)
        if status != 0:
            logger.warn('request failed (%s): %s' % (status, output))
            return None
    else:
        req = urllib2.Request(url, urllib.urlencode(data))
        if not plain:
            req.add_header('Accept', 'application/json')
        req.add_header('User-Agent', _ctx.user_agent)
        try:
            output = urllib2.urlopen(req, context=_ctx.ssl_context)
        except urllib2.HTTPError as e:
            logger.warn('server error (%s): %s' % (e.code, e.read()))
            return None
        except urllib2.URLError as e:
            logger.warn('connection error: %s' % e.reason)
            return None

    return output if plain else json.loads(output)
