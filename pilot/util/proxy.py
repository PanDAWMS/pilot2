#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019

from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def get_distinguished_name():
    """
    Get the user DN.
    Note: the DN is also sent by the server to the pilot in the job description (produserid).

    :return: User DN (string).
    """

    dn = ""
    executable = 'arcproxy -i subject'
    exit_code, stdout, stderr = execute(executable)
    if exit_code != 0 or "ERROR:" in stderr:
        logger.warning("arcproxy failed: ec=%d, stdout=%s, stderr=%s" % (exit_code, stdout, stderr))

        if "command not found" in stderr or "Can not find certificate file" in stderr:
            logger.warning("arcproxy experienced a problem (will try voms-proxy-info instead)")

            # Default to voms-proxy-info
            executable = 'voms-proxy-info -subject'
            exit_code, stdout, stderr = execute(executable)

    if exit_code == 0:
        dn = stdout
        logger.info('DN = %s' % dn)
        cn = "/CN=proxy"
        if not dn.endswith(cn):
            logger.info("DN does not end with %s (will be added)" % cn)
            dn += cn

    else:
        logger.warning("user=self set but cannot get proxy: %d, %s" % (exit_code, stdout))

    return dn
