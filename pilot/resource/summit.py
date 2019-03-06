#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

import logging
logger = logging.getLogger(__name__)


def get_setup(job=None):
    """
    Return the resource specific setup.

    :param job: optional job object.
    :return: setup commands (list).
    """

    return []
