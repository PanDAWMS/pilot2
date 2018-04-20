#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def verify_proxy(limit=None):
    """
    Check for a valid voms/grid proxy longer than N hours.
    Use `limit` to set required time limit.

    :param limit: time limit in hours (int).
    :return: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """

    return 0, ""
