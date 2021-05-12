# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

#import re

#import logging

#logger = logging.getLogger(__name__)


def jobparams_prefiltering(value):
    """
    Perform pre-filtering of raw job parameters to avoid problems with especially quotation marks.
    The function can extract some fields from the job parameters to be put back later after actual filtering.

    E.g. ' --athenaopts "HITtoRDO:--nprocs=$ATHENA_CORE_NUMBER" ' will otherwise become
    ' --athenaopts 'HITtoRDO:--nprocs=$ATHENA_CORE_NUMBER' ' which will prevent the environmental variable to be unfolded.

    :param value: job parameters (string).
    :return: list of fields excluded from job parameters (list), updated job parameters (string).
    """

    exclusions = {}

    # Add regex patterns here

    return exclusions, value


def jobparams_postfiltering(value, exclusions={}):
    """
    Perform post-filtering of raw job parameters.
    Any items in the optional exclusion list will be added (space separated) at the end of the job parameters.

    :param value: job parameters (string).
    :param optional exclusions: exlusions dictionary from pre-filtering function (dictionary).
    :return: updated job parameters (string).
    """

    return value
