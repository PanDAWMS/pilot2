#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018
# - Alexander Bogdanchikov, Alexander.Bogdanchikov@cern.ch, 2020

# from pilot.util.container import execute

import os
import logging

from pilot.user.atlas.setup import get_file_system_root_path
from pilot.util.container import execute
from pilot.common.errorcodes import ErrorCodes
from time import time

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def verify_proxy(limit=None, x509=None, proxy_id="pilot"):
    """
    Check for a valid voms/grid proxy longer than N hours.
    Use `limit` to set required time limit.

    :param limit: time limit in hours (int).
    :param x509: points to the proxy file. If not set (=None) - get proxy file from X509_USER_PROXY environment
    :return: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """

    exit_code = 0
    diagnostics = ""

    logger.debug("verify_proxy() begin")

    if limit is None:
        limit = 48

    # add setup for arcproxy if it exists
    #arcproxy_setup = "%s/atlas.cern.ch/repo/sw/arc/client/latest/slc6/x86_64/setup.sh" % get_file_system_root_path()
    if x509 is None:
        x509 = os.environ.get('X509_USER_PROXY', '')
    if x509 != '':
        envsetup = 'export X509_USER_PROXY=%s;' % x509
    else:
        envsetup = ''
    #envsetup += ". %s;" % (arcproxy_setup)
    envsetup += ". %s/atlas.cern.ch/repo/ATLASLocalRootBase/user/atlasLocalSetup.sh --quiet;" % get_file_system_root_path()
    if os.environ.get('ALRB_noGridMW', '').lower() != "yes":
        envsetup += "lsetup emi;"
        #envsetup += "export; ls -l $X509_USER_PROXY; lsetup emi; export; ls -l $X509_USER_PROXY;"
    else:
        logger.warning('Skipping "lsetup emi" as ALRB_noGridMW=YES')

    # first try to use arcproxy since voms-proxy-info is not working properly on SL6
    #  (memory issues on queues with limited memory)

    ec, diagnostics = verify_arcproxy(envsetup, limit, proxy_id)
    if ec != 0 and ec != -1:
        return ec, diagnostics
    elif ec == -1:
        pass  # go to next test
    else:
        return 0, diagnostics

    ec, diagnostics = verify_vomsproxy(envsetup, limit)
    if ec != 0:
        return ec, diagnostics
    else:
        return 0, diagnostics

    ec, diagnostics = verify_gridproxy(envsetup, limit)
    if ec != 0:
        return ec, diagnostics

    return exit_code, diagnostics


def verify_arcproxy(envsetup, limit, proxy_id="pilot"):
    """
    Verify the proxy using arcproxy.

    :param envsetup: general setup string for proxy commands (string).
    :param limit: time limit in hours (int).
    :param  proxy_id: proxy unique id name. The verification result will be cached for this id. If None the result will not be cached (string)
    :return: exit code (int), error diagnostics (string).
    """
    ec = 0
    diagnostics = ""

    logger.debug("verify_arcproxy() begin")

    if proxy_id is not None:
        if not hasattr(verify_arcproxy, "cache"):
            verify_arcproxy.cache = {}
        logger.info("current cache=%s" % (verify_arcproxy.cache))
        if proxy_id in verify_arcproxy.cache:  # if exist, then calculate result from current cache
            validity_end = verify_arcproxy.cache[proxy_id]
            if validity_end < 0:  # previous validity check failed, do not try to re-check
                ec = -1
                diagnostics = "arcproxy verification failed (cached result)"
            else:
                tnow = int(time() + 0.5)  # round to seconds
                seconds_left = validity_end - tnow
                logger.info("cache: check '%s' proxy validity: wanted=%dh left=%.2fh (now=%d validity_end=%d left=%d)"
                            % (proxy_id, limit, float(seconds_left) / 3600, tnow, validity_end, seconds_left))
                if seconds_left < limit * 3600:
                    logger.info("'%s' proxy validity time is too short" % (proxy_id))
                    ec = -1
                    diagnostics = "validity time is too short"
                else:
                    logger.info("'%s' proxy validity time is enough" % (proxy_id))
            return ec, diagnostics

    # options and options' sequence are important for parsing, do not change it
    cmd = "%sarcproxy -i vomsACvalidityEnd -i vomsACvalidityLeft" % (envsetup)

    exit_code, stdout, stderr = execute(cmd, shell=True)  # , usecontainer=True, copytool=True)
    if stdout is not None:
        if 'command not found' in stdout:
            logger.warning("arcproxy is not available on this queue,"
                           "this can lead to memory issues with voms-proxy-info on SL6: %s" % (stdout))
            ec = -1
        else:
            ec, diagnostics, validity_end = interpret_proxy_info(exit_code, stdout, stderr, limit)
            if proxy_id and validity_end:  # setup cache if requered
                if ec == 0:
                    logger.info("cache the validity_end: cache['%s'] = %d" % (proxy_id, validity_end))
                    verify_arcproxy.cache[proxy_id] = validity_end
                else:
                    verify_arcproxy.cache[proxy_id] = -1  # -1 in cache means any error in prev validation
            if ec == 0:
                logger.info("voms proxy verified using arcproxy")
                return 0, diagnostics
            elif ec == -1:  # skip to next proxy test
                return ec, diagnostics
            elif ec == errors.NOVOMSPROXY:
                return ec, diagnostics
            else:
                logger.info("will try voms-proxy-info instead")
                ec = -1
    else:
        logger.warning('command execution failed')

    return ec, diagnostics


def verify_vomsproxy(envsetup, limit):
    """
    Verify proxy using voms-proxy-info command.

    :param envsetup: general setup string for proxy commands (string).
    :param limit: time limit in hours (int).
    :return: exit code (int), error diagnostics (string).
    """

    ec = 0
    diagnostics = ""

    logger.debug("verify_arcproxy() begin")

    if os.environ.get('X509_USER_PROXY', '') != '':
        cmd = "%svoms-proxy-info -actimeleft --file $X509_USER_PROXY" % (envsetup)
        logger.info('executing command: %s' % cmd)
        exit_code, stdout, stderr = execute(cmd, shell=True)
        if stdout is not None:
            if "command not found" in stdout:
                logger.info("skipping voms proxy check since command is not available")
            else:
                ec, diagnostics, validity_end = interpret_proxy_info(exit_code, stdout, stderr, limit)
                if ec == 0:
                    logger.info("voms proxy verified using voms-proxy-info")
                    return 0, diagnostics
        else:
            logger.warning('command execution failed')
    else:
        logger.warning('X509_USER_PROXY is not set')

    return ec, diagnostics


def verify_gridproxy(envsetup, limit):
    """
    Verify proxy using grid-proxy-info command.

    :param envsetup: general setup string for proxy commands (string).
    :param limit: time limit in hours (int).
    :return: exit code (int), error diagnostics (string).
    """

    ec = 0
    diagnostics = ""

    if limit:
        # next clause had problems: grid-proxy-info -exists -valid 0.166666666667:00
        #cmd = "%sgrid-proxy-info -exists -valid %s:00" % (envsetup, str(limit))
        # more accurate calculation of HH:MM
        limit_hours = int(limit * 60) / 60
        limit_minutes = int(limit * 60 + .999) - limit_hours * 60
        cmd = "%sgrid-proxy-info -exists -valid %d:%02d" % (envsetup, limit_hours, limit_minutes)
    else:
        cmd = "%sgrid-proxy-info -exists -valid 24:00" % (envsetup)

    logger.info('executing command: %s' % cmd)
    exit_code, stdout, stderr = execute(cmd, shell=True)
    if stdout is not None:
        if exit_code != 0:
            if stdout.find("command not found") > 0:
                logger.info("skipping grid proxy check since command is not available")
            else:
                # Analyze exit code / stdout
                diagnostics = "grid proxy certificate does not exist or is too short: %d, %s" % (exit_code, stdout)
                logger.warning(diagnostics)
                return errors.NOPROXY, diagnostics
        else:
            logger.info("grid proxy verified")
    else:
        logger.warning('command execution failed')

    return ec, diagnostics


def interpret_proxy_info(ec, stdout, stderr, limit):
    """
    Interpret the output from arcproxy or voms-proxy-info.

    :param ec: exit code from proxy command (int).
    :param stdout: stdout from proxy command (string).
    :param stderr: stderr from proxy command (string).
    :param limit: time limit in hours (int).
    :return: exit code (int), diagnostics (string). validity end in seconds if detected, None if not detected(int)
    """

    exitcode = 0
    diagnostics = ""
    validity_end = None  # not detected

    logger.debug('stdout = %s' % stdout)
    logger.debug('stderr = %s' % stderr)

    if ec != 0:
        if "Unable to verify signature! Server certificate possibly not installed" in stdout:
            logger.warning("skipping voms proxy check: %s" % (stdout))
        # test for command errors
        elif "arcproxy: error while loading shared libraries" in stderr:
            exitcode = -1
            logger.warning('skipping arcproxy test')
        elif "arcproxy:" in stdout:
            diagnostics = "arcproxy failed: %s" % (stdout)
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR
        else:
            # Analyze exit code / output
            diagnostics = "voms proxy certificate check failure: %d, %s" % (ec, stdout)
            logger.warning(diagnostics)
            exitcode = errors.NOVOMSPROXY
    else:
        # remove any additional print-outs if present, assume that the last line is the time left
        if "\n" in stdout:
            # also remove the last \n in case there is one
            if stdout[-1] == '\n':
                stdout = stdout[:-1]
            stdout_split = stdout.split('\n')
            logger.debug("splitted stdout = %s" % (stdout_split))
            # try to get validity_end in penult line, it may fail, throw exception
            try:
                validity_end_str = stdout_split[-2]  # imay raise exception IndexError if stdout is too short
                logger.debug("try to get validity_end from the line: \"%s\"" % validity_end_str)
                validity_end = int(validity_end_str)  # may raise ValueError if not string
                logger.info("validity_end = %d" % (validity_end))
            except (IndexError, ValueError) as err:
                logger.info("validity_end not found in stdout (%s)" % err)
                pass
            stdout = stdout_split[-1]  # remove everything except last line

        # test for command errors
        if "arcproxy:" in stdout:
            diagnostics = "arcproxy failed: %s" % (stdout)
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR
        else:
            # on EMI-3 the time output is different (HH:MM:SS as compared to SS on EMI-2)
            if ":" in stdout:
                ftr = [3600, 60, 1]
                stdout = sum([a * b for a, b in zip(ftr, list(map(int, stdout.split(':'))))])  # Python 2/3
            try:
                validity = int(stdout)
                if validity >= limit * 3600:
                    logger.info("voms proxy verified (%d s)" % (validity))
                else:
                    diagnostics = "voms proxy certificate does not exist or is too short (lifetime %d s)" % (validity)
                    logger.warning(diagnostics)
                    exitcode = errors.NOVOMSPROXY
            except ValueError as e:
                diagnostics = "failed to evalute command stdout: %s, stderr: %s, exc=%s" % (stdout, stderr, e)
                logger.warning(diagnostics)
                exitcode = errors.GENERALERROR

    return exitcode, diagnostics, validity_end
