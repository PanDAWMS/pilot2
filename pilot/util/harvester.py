#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from os import environ
from os.path import exists, join

from pilot.util.filehandling import read_json, write_json
from pilot.util.config import config
from pilot.common.exception import FileHandlingFailure

import logging
logger = logging.getLogger(__name__)


def add_id_and_state_to_json(pandaid, state):
    """
    Add PanDA id and final job state to Harvester dictionary.

    :param pandaid:
    :return:
    """

    # get the Harvester json if it already exists
    path = join(environ['PILOT_HOME'], config.Harvester.completed_pandaids)
    if not exists(path):
        dictionary = {}
    else:
        try:
            dictionary = read_json(path)
        except FileHandlingFailure:
            raise FileHandlingFailure
        except ConversionFailure:
            raise ConversionFailure

    # add the new pandaid and its state
    dictionary[pandaid] = state

    # write it to file
    try:
        write_json(path, dictionary)
    except FileHandlingFailure:
        raise FileHandlingFailure
