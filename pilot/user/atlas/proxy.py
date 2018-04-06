#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# from pilot.util.container import execute

from pilot.user.atlas.setup import get_file_system_root_path
from pilot.util.container import execute

import logging

logger = logging.getLogger(__name__)


def verify_proxy(limit=None):
    """
    Check for a valid voms/grid proxy longer than N hours.
    Use `limit` to set required time limit.

    :param limit: time limit in hours (int).
    :return: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """

    exitcode = 0
    diagnostics = ""

    if limit == None:
        limit = 48

    # add setup for arcproxy if it exists
    arcproxy_setup = "%s/atlas.cern.ch/repo/sw/arc/client/latest/slc6/x86_64/setup.sh" % get_file_system_root_path()
    envsetup += ". %s;" % (arcproxy_setup)

    # first try to use arcproxy since voms-proxy-info is not working properly on SL6
    #  (memory issues on queues with limited memory)

    cmd = "%sarcproxy -i vomsACvalidityLeft" % (envsetup)

    tolog("Executing command: %s" % (cmd))
    exitcode, output = commands.getstatusoutput(cmd)
    if "command not found" in output:
        tolog("!!WARNING!!1234!! arcproxy is not available on this queue, this can lead to memory issues with voms-proxy-info on SL6: %s" % (output))
    else:
        ec, pilotErrorDiag = self.interpretProxyInfo(exitcode, output, limit)
        if ec == 0:
            tolog("Voms proxy verified using arcproxy")
            return 0, pilotErrorDiag
        elif ec == error.ERR_NOVOMSPROXY:
            return ec, pilotErrorDiag
        else:
            tolog("Will try voms-proxy-info instead")

    # -valid HH:MM is broken
    if "; ;" in envsetup:
        envsetup = envsetup.replace('; ;', ';')
        tolog("Removed a double ; from envsetup")
    cmd = "%svoms-proxy-info -actimeleft --file $X509_USER_PROXY" % (envsetup)
    tolog("Executing command: %s" % (cmd))
    exitcode, output = commands.getstatusoutput(cmd)
    if "command not found" in output:
        tolog("Skipping voms proxy check since command is not available")
    else:
        ec, pilotErrorDiag = self.interpretProxyInfo(exitcode, output, limit)
        if ec == 0:
            tolog("Voms proxy verified using voms-proxy-info")
            return 0, pilotErrorDiag

    if limit:
        # next clause had problems: grid-proxy-info -exists -valid 0.166666666667:00
        #cmd = "%sgrid-proxy-info -exists -valid %s:00" % (envsetup, str(limit))
        # more accurate calculation of HH:MM
        limit_hours=int(limit*60)/60
        limit_minutes=int(limit*60+.999)-limit_hours*60
        cmd = "%sgrid-proxy-info -exists -valid %d:%02d" % (envsetup, limit_hours, limit_minutes)
    else:
        cmd = "%sgrid-proxy-info -exists -valid 24:00" % (envsetup)
    tolog("Executing command: %s" % (cmd))
    exitcode, output = commands.getstatusoutput(cmd)
    if exitcode != 0:
        if output.find("command not found") > 0:
            tolog("Skipping grid proxy check since command is not available")
        else:
            # Analyze exit code / output
            from futil import check_syserr
            check_syserr(exitcode, output)
            pilotErrorDiag = "Grid proxy certificate does not exist or is too short: %d, %s" % (exitcode, output)
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return error.ERR_NOPROXY, pilotErrorDiag
    else:
        tolog("Grid proxy verified")

#    return 0, pilotErrorDiag

    return exitcode, diagnostics
