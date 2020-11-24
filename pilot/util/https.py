#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2020

import collections
import subprocess  # Python 2/3
try:
    import commands  # Python 2
except Exception:
    pass
import json
import os
import platform
import ssl
import sys
try:
    import urllib.request  # Python 3
    import urllib.error  # Python 3
    import urllib.parse  # Python 3
except Exception:
    import urllib  # Python 2
    import urllib2  # Python 2
import pipes

from .filehandling import write_file
from .auxiliary import is_python3
from .config import config
from .constants import get_pilot_version

import logging
logger = logging.getLogger(__name__)

_ctx = collections.namedtuple('_ctx', 'ssl_context user_agent capath cacert')

# anisyonk: public copy of `_ctx` to avoid logic break since ssl_context is reset inside the request() -- FIXME
# anisyonk: public instance, should be properly initialized by `https_setup()`
# anisyonk: use lightweight class definition instead of namedtuple since tuple is immutable and we don't need/use any tuple features here
ctx = type('ctx', (object,), dict(ssl_context=None, user_agent='Pilot2 client', capath=None, cacert=None))


def _tester(func, *args):
    """
    Tests function ``func`` on arguments and returns first positive.

    >>> _tester(lambda x: x%3 == 0, 1, 2, 3, 4, 5, 6)
    3
    >>> _tester(lambda x: x%3 == 0, 1, 2)
    None

    :param func: function(arg)->boolean
    :param args: other arguments
    :return: something or none
    """
    for arg in args:
        if arg is not None and func(arg):
            return arg

    return None


def capath(args=None):
    """
    Tries to get :abbr:`CA (Certification Authority)` path with certificates.
    Testifies it to be a directory.
    Tries next locations:

    1. :option:`--capath` from arguments
    2. :envvar:`X509_CERT_DIR` from env
    3. Path ``/etc/grid-security/certificates``

    :param args: arguments, parsed by `argparse`
    :returns: `str` -- directory path, or `None`
    """

    return _tester(os.path.isdir,
                   args and args.capath,
                   os.environ.get('X509_CERT_DIR'),
                   '/etc/grid-security/certificates')


def cacert_default_location():
    """
    Tries to get current user ID through `os.getuid`, and get the posix path for x509 certificate.
    :returns: `str` -- posix default x509 path, or `None`
    """
    try:
        return '/tmp/x509up_u%s' % str(os.getuid())
    except AttributeError:
        logger.warn('No UID available? System not POSIX-compatible... trying to continue')
        pass

    return None


def cacert(args=None):
    """
    Tries to get :abbr:`CA (Certification Authority)` certificate or X509 one.
    Testifies it to be a regular file.
    Tries next locations:

    1. :option:`--cacert` from arguments
    2. :envvar:`X509_USER_PROXY` from env
    3. Path ``/tmp/x509up_uXXX``, where ``XXX`` refers to ``UID``

    :param args: arguments, parsed by `argparse`
    :returns: `str` -- certificate file path, or `None`
    """

    return _tester(os.path.isfile,
                   args and args.cacert,
                   os.environ.get('X509_USER_PROXY'),
                   cacert_default_location())


def https_setup(args=None, version=None):
    """
    Sets up the context for future HTTPS requests:

    1. Selects the certificate paths
    2. Sets up :mailheader:`User-Agent`
    3. Tries to create `ssl.SSLContext` for future use (falls back to :command:`curl` if fails)

    :param args: arguments, parsed by `argparse`
    :param str version: pilot version string (for :mailheader:`User-Agent`)
    """

    version = version or get_pilot_version()

    _ctx.user_agent = 'pilot/%s (Python %s; %s %s)' % (version,
                                                       sys.version.split()[0],
                                                       platform.system(),
                                                       platform.machine())
    logger.debug('User-Agent: %s' % _ctx.user_agent)

    _ctx.capath = capath(args)
    _ctx.cacert = cacert(args)

    if sys.version_info < (2, 7, 9):  # by anisyonk: actually SSL context should work, but prior to 2.7.9 there is no automatic hostname/certificate validation
        logger.warn('Python version <2.7.9 lacks SSL contexts -- falling back to curl')
        _ctx.ssl_context = None
    else:
        try:
            _ctx.ssl_context = ssl.create_default_context(capath=_ctx.capath,
                                                          cafile=_ctx.cacert)
        except Exception as e:
            logger.warn('SSL communication is impossible due to SSL error: %s -- falling back to curl' % str(e))
            _ctx.ssl_context = None

    # anisyonk: clone `_ctx` to avoid logic break since ssl_context is reset inside the request() -- FIXME
    ctx.capath = _ctx.capath
    ctx.cacert = _ctx.cacert
    ctx.user_agent = _ctx.user_agent

    try:
        ctx.ssl_context = ssl.create_default_context(capath=ctx.capath, cafile=ctx.cacert)
        ctx.ssl_context.load_cert_chain(ctx.cacert)
    except Exception as e:  # redandant try-catch protection, should work well for both python2 & python3 -- CLEAN ME later (anisyonk)
        logger.warn('Failed to initialize SSL context .. skipped, error: %s' % str(e))


def request(url, data=None, plain=False, secure=True):
    """
    This function sends a request using HTTPS.
    Sends :mailheader:`User-Agent` and certificates previously being set up by `https_setup`.
    If `ssl.SSLContext` is available, uses `urllib2` as a request processor. Otherwise uses :command:`curl`.

    If ``data`` is provided, encodes it as a URL form data and sends it to the server.

    Treats the request as JSON unless a parameter ``plain`` is `True`.
    If JSON is expected, sends ``Accept: application/json`` header.

    :param string url: the URL of the resource
    :param dict data: data to send
    :param boolean plain: if true, treats the response as a plain text.
    :param secure: Boolean (default: True, ie use certificates)
    Usage:

    .. code-block:: python
        :emphasize-lines: 2

        https_setup(args, PILOT_VERSION)  # sets up ssl and other stuff
        response = request('https://some.url', {'some':'data'})

    Returns:
        - :keyword:`dict` -- if everything went OK
        - `str` -- if ``plain`` parameter is `True`
        - `None` -- if something went wrong
    """

    _ctx.ssl_context = None  # certificates are not available on the grid, use curl

    logger.debug('server update dictionary = \n%s' % str(data))

    # get the filename and strdata for the curl config file
    filename, strdata = get_vars(url, data)
    # write the strdata to file
    writestatus = write_file(filename, strdata)
    # get the config option for the curl command
    dat = get_curl_config_option(writestatus, url, data, filename)

    if _ctx.ssl_context is None and secure:
        req = get_curl_command(plain, dat)

        try:
            status, output = execute_request(req)
        except Exception as e:
            logger.warning('exception: %s' % e)
            return None
        else:
            if status != 0:
                logger.warn('request failed (%s): %s' % (status, output))
                return None

        # return output if plain otherwise return json.loads(output)
        if plain:
            return output
        else:
            try:
                ret = json.loads(output)
            except Exception as e:
                logger.warning('json.loads() failed to parse output=%s: %s' % (output, e))
                return None
            else:
                return ret
    else:
        req = execute_urllib(url, data, plain, secure)
        context = _ctx.ssl_context if secure else None

        if is_python3():  # Python 3
            ec, output = get_urlopen_output(req, context)
            if ec:
                return None
        else:  # Python 2
            ec, output = get_urlopen2_output(req, context)
            if ec:
                return None

        return output.read() if plain else json.load(output)


def get_curl_command(plain, dat):
    """
    Get the curl command.

    :param plain:
    :param dat: curl config option (string).
    :return: curl command (string).
    """
    req = 'curl -sS --compressed --connect-timeout %s --max-time %s '\
          '--capath %s --cert %s --cacert %s --key %s '\
          '-H %s %s %s' % (config.Pilot.http_connect_timeout, config.Pilot.http_maxtime,
                           pipes.quote(_ctx.capath or ''), pipes.quote(_ctx.cacert or ''),
                           pipes.quote(_ctx.cacert or ''), pipes.quote(_ctx.cacert or ''),
                           pipes.quote('User-Agent: %s' % _ctx.user_agent),
                           "-H " + pipes.quote('Accept: application/json') if not plain else '',
                           dat)
    logger.info('request: %s' % req)
    return req


def get_vars(url, data):
    """
    Get the filename and strdata for the curl config file.

    :param url: URL (string).
    :param data: data to be written to file (dictionary).
    :return: filename (string), strdata (string).
    """

    strdata = ""
    for key in data:
        try:
            strdata += 'data="%s"\n' % urllib.parse.urlencode({key: data[key]})  # Python 3
        except Exception:
            strdata += 'data="%s"\n' % urllib.urlencode({key: data[key]})  # Python 2
    jobid = ''
    if 'jobId' in list(data.keys()):  # Python 2/3
        jobid = '_%s' % data['jobId']

    # write data to temporary config file
    filename = '%s/curl_%s%s.config' % (os.getenv('PILOT_HOME'), os.path.basename(url), jobid)

    return filename, strdata


def get_curl_config_option(writestatus, url, data, filename):
    """
    Get the curl config option.

    :param writestatus: status of write_file call (Boolean).
    :param url: URL (string).
    :param data: data structure (dictionary).
    :param filename: file name of config file (string).
    :return: config option (string).
    """

    if not writestatus:
        logger.warning('failed to create curl config file (will attempt to urlencode data directly)')
        try:
            dat = pipes.quote(url + '?' + urllib.parse.urlencode(data) if data else '')  # Python 3
        except Exception:
            dat = pipes.quote(url + '?' + urllib.urlencode(data) if data else '')  # Python 2
    else:
        dat = '--config %s %s' % (filename, url)

    return dat


def execute_request(req):
    """
    Execute the curl request.

    :param req: curl request command (string).
    :return: status (int), output (string).
    """
    try:
        status, output = subprocess.getstatusoutput(req)  # Python 3
    except Exception:
        status, output = commands.getstatusoutput(req)  # Python 2
    return status, output


def execute_urllib(url, data, plain, secure):
    """
    Execute the request using urllib.

    :param url: URL (string).
    :param data: data structure
    :return: urllib request structure.
    """
    try:
        req = urllib.request.Request(url, urllib.parse.urlencode(data))  # Python 3
    except Exception:
        req = urllib2.Request(url, urllib.urlencode(data))  # Python 2

    if not plain:
        req.add_header('Accept', 'application/json')
    if secure:
        req.add_header('User-Agent', _ctx.user_agent)

    return req


def get_urlopen_output(req, context):
    """
    Get the output from the urlopen request.

    :param req:
    :param context:
    :return: ec (int), output (string).
    """

    ec = -1
    output = ""
    try:
        output = urllib.request.urlopen(req, context=context)
    except urllib.error.HTTPError as e:
        logger.warn('server error (%s): %s' % (e.code, e.read()))
    except urllib.error.URLError as e:
        logger.warn('connection error: %s' % e.reason)
    else:
        ec = 0

    return ec, output


def get_urlopen2_output(req, context):
    """
    Get the output from the urlopen2 request.

    :param req:
    :param context:
    :return: ec (int), output (string).
    """

    ec = -1
    output = ""
    try:
        output = urllib2.urlopen(req, context=context)
    except urllib2.HTTPError as e:
        logger.warn('server error (%s): %s' % (e.code, e.read()))
    except urllib2.URLError as e:
        logger.warn('connection error: %s' % e.reason)
    else:
        ec = 0

    return ec, output
