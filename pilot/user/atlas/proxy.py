#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# from pilot.util.container import execute

import os
import logging

from pilot.user.atlas.setup import get_file_system_root_path
from pilot.util.container import execute
from pilot.common.errorcodes import ErrorCodes

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def verify_proxy(limit=None):
    """
    Check for a valid voms/grid proxy longer than N hours.
    Use `limit` to set required time limit.

    :param limit: time limit in hours (int).
    :return: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """

    diagnostics = ""

    if limit is None:
        limit = 48

    # add setup for arcproxy if it exists
    arcproxy_setup = "%s/atlas.cern.ch/repo/sw/arc/client/latest/slc6/x86_64/setup.sh" % get_file_system_root_path()
    x509 = os.environ.get('X509_USER_PROXY', '')
    if x509 != '':
        envsetup = 'export X509_USER_PROXY=%s;' % x509
    else:
        envsetup = ''
    envsetup += ". %s;" % (arcproxy_setup)

    # first try to use arcproxy since voms-proxy-info is not working properly on SL6
    #  (memory issues on queues with limited memory)

    cmd = "%sarcproxy -i vomsACvalidityLeft" % (envsetup)

    exit_code, stdout, stderr = execute(cmd, shell=True)
    if stdout is not None:
        if "command not found" in stdout:
            logger.warning("arcproxy is not available on this queue,"
                           "this can lead to memory issues with voms-proxy-info on SL6: %s" % (stdout))
        else:
            ec, diagnostics = interpret_proxy_info(exit_code, stdout, stderr, limit)
            if ec == 0:
                logger.info("voms proxy verified using arcproxy")
                return 0, diagnostics
            elif ec == errors.NOVOMSPROXY:
                return ec, diagnostics
            else:
                logger.info("will try voms-proxy-info instead")
    else:
        logger.warning('command execution failed')

    # -valid HH:MM is broken
    if "; ;" in envsetup:
        envsetup = envsetup.replace('; ;', ';')

    if os.environ.get('X509_USER_PROXY', '') != '':
        cmd = "%svoms-proxy-info -actimeleft --file $X509_USER_PROXY" % (envsetup)
        logger.info('executing command: %s' % cmd)
        exit_code, stdout, stderr = execute(cmd, shell=True)
        if stdout is not None:
            if "command not found" in stdout:
                logger.info("skipping voms proxy check since command is not available")
            else:
                ec, diagnostics = interpret_proxy_info(exit_code, stdout, stderr, limit)
                if ec == 0:
                    logger.info("voms proxy verified using voms-proxy-info")
                    return 0, diagnostics
        else:
            logger.warning('command execution failed')
    else:
        logger.warning('X509_USER_PROXY is not set')

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

    return exit_code, diagnostics


def interpret_pr#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# from pilot.util.container import execute

import os
import logging

from pilot.user.atlas.setup import get_file_system_root_path
from pilot.util.container import execute
from pilot.common.errorcodes import ErrorCodes

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def verify_proxy(limit=None):
    """
    Check for a valid voms/grid proxy longer than N hours.
    Use `limit` to set required time limit.

    :param limit: time limit in hours (int).
    :return: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """

    diagnostics = ""

    if limit is None:
        limit = 48

    # add setup for arcproxy if it exists
    arcproxy_setup = "%s/atlas.cern.ch/repo/sw/arc/client/latest/slc6/x86_64/setup.sh" % get_file_system_root_path()
    x509 = os.environ.get('X509_USER_PROXY', '')
    if x509 != '':
        envsetup = 'export X509_USER_PROXY=%s;' % x509
    else:
        envsetup = ''
    envsetup += ". %s;" % (arcproxy_setup)

    # first try to use arcproxy since voms-proxy-info is not working properly on SL6
    #  (memory issues on queues with limited memory)

    cmd = "%sarcproxy -i vomsACvalidityLeft" % (envsetup)

    exit_code, stdout, stderr = execute(cmd, shell=True)
    if stdout is not None:
        if "command not found" in stdout:
            logger.warning("arcproxy is not available on this queue,"
                           "this can lead to memory issues with voms-proxy-info on SL6: %s" % (stdout))
        else:
            ec, diagnostics = interpret_proxy_info(exit_code, stdout, stderr, limit)
            if ec == 0:
                logger.info("voms proxy verified using arcproxy")
                return 0, diagnostics
            elif ec == errors.NOVOMSPROXY:
                return ec, diagnostics
            else:
                logger.info("will try voms-proxy-info instead")
    else:
        logger.warning('command execution failed')

    # -valid HH:MM is broken
    if "; ;" in envsetup:
        envsetup = envsetup.replace('; ;', ';')

    if os.environ.get('X509_USER_PROXY', '') != '':
        cmd = "%svoms-proxy-info -actimeleft --file $X509_USER_PROXY" % (envsetup)
        logger.info('executing command: %s' % cmd)
        exit_code, stdout, stderr = execute(cmd, shell=True)
        if stdout is not None:
            if "command not found" in stdout:
                logger.info("skipping voms proxy check since command is not available")
            else:
                ec, diagnostics = interpret_proxy_info(exit_code, stdout, stderr, limit)
                if ec == 0:
                    logger.info("voms proxy verified using voms-proxy-info")
                    return 0, diagnostics
        else:
            logger.warning('command execution failed')
    else:
        logger.warning('X509_USER_PROXY is not set')

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

    return exit_code, diagnostics


def interpret_proxy_info(ec, stdout, stderr, limit):
    """
    Interpret the output from arcproxy or voms-proxy-info.

    :param ec: exit code from proxy command (int).
    :param stdout: stdout from proxy command (string).
    :param stderr: stderr from proxy command (string).
    :param limit: time limit in hours (int).
    :return: exit code (int), diagnostics (string).
    """

    exitcode = 0
    diagnostics = ""

    logger.debug('stdout = %s' % stdout)
    logger.debug('stderr = %s' % stderr)

    if ec != 0:
        if "Unable to verify signature! Server certificate possibly not installed" in stdout:
            logger.warning("skipping voms proxy check: %s" % (stdout))
        # test for command errors
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
            stdout = stdout.split('\n')[-1]
        logger.debug('stdout(2) = %s' % stdout)

        # test for command errors
        if "arcproxy:" in stdout:
            diagnostics = "arcproxy failed: %s" % (stdout)
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR
        else:
            # on EMI-3 the time output is different (HH:MM:SS as compared to SS on EMI-2)
            if ":" in stdout:
                ftr = [3600, 60, 1]
                stdout = sum([a * b for a, b in zip(ftr, map(int, stdout.split(':')))])
            try:
                validity = int(stdout)
                if validity >= limit * 3600:
                    logger.info("voms proxy verified (%ds)" % (validity))
                else:
                    diagnostics = "voms proxy certificate does not exist or is too short (lifetime %d s)" % (validity)
                    logger.warning(diagnostics)
                    exitcode = errors.NOVOMSPROXY
            except ValueError as e:
                diagnostics = "failed to evalute command stdout: %s, stderr: %s, exc=%s" % (stdout, stderr, e)
                logger.warning(diagnostics)
                exitcode = errors.GENERALERROR

    return exitcode, diagnostics
oxy_info(ec, stdout, stderr, limit):
    """
    Interpret the output from arcproxy or voms-proxy-info.

    :param ec: exit code from proxy command (int).
    :param stdout: stdout from proxy command (string).
    :param stderr: stderr from proxy command (string).
    :param limit: time limit in hours (int).
    :return: exit code (int), diagnostics (string).
    """

    exitcode = 0
    diagnostics = ""

    logger.debug('stdout = %s' % stdout)
    logger.debug('stderr = %s' % stderr)

    if ec != 0:
        if "Unable to verify signature! Server certificate possibly not installed" in stdout:
            logger.warning("skipping voms proxy check: %s" % (stdout))
        # test for command errors
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
            stdout = stdout.split('\n')[-1]

        # test for command errors
        if "arcproxy:" in stdout:
            diagnostics = "arcproxy failed: %s" % (stdout)
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR
        else:
            # on EMI-3 the time output is different (HH:MM:SS as compared to SS on EMI-2)
            if ":" in stdout:
                ftr = [3600, 60, 1]
                stdout = sum([a * b for a, b in zip(ftr, map(int, stdout.split(':')))])
            try:
                validity = int(stdout)
                if validity >= limit * 3600:
                    logger.info("voms proxy verified (%ds)" % (validity))
                else:
                    diagnostics = "voms proxy certificate does not exist or is too short (lifetime %d s)" % (validity)
                    logger.warning(diagnostics)
                    exitcode = errors.NOVOMSPROXY
            except ValueError:
                diagnostics = "failed to evalute command stdout: %s, stderr: %s" % (stdout, stderr)
                logger.warning(diagnostics)
                exitcode = errors.GENERALERROR

    return exitcode, diagnostics
