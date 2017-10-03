#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import subprocess

import logging
logger = logging.getLogger(__name__)


def copy_in(files):
    """
    Tries to download the given files using mv directly.

    :param files: Files to download

    :raises Exception
    """
    raise NotImplementedError()


def copy_out(files):
    """
    Tries to upload the given files using xrdcp directly.

    :param files Files to download

    :raises Exception
    """
    raise NotImplementedError()
