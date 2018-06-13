#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import os
import re
import fnmatch
from collections import defaultdict
from glob import glob
from signal import SIGTERM, SIGUSR1

from pilot.common.exception import TrfDownloadFailure
from pilot.user.atlas.setup import should_pilot_prepare_asetup, get_asetup, get_asetup_options, is_standard_atlas_job,\
    set_inds, get_analysis_trf, get_payload_environment_variables
from pilot.user.atlas.utilities import get_memory_monitor_setup, get_network_monitor_setup, post_memory_monitor_action,\
    get_memory_monitor_summary_filename, get_prefetcher_setup, get_benchmark_setup
from pilot.util.auxiliary import get_logger
from pilot.util.constants import UTILITY_BEFORE_PAYLOAD, UTILITY_WITH_PAYLOAD, UTILITY_AFTER_PAYLOAD,\
    UTILITY_WITH_STAGEIN
from pilot.util.container import execute
from pilot.util.filehandling import remove

import logging
logger = logging.getLogger(__name__)


class Services(object):
    """
    High-level base class for Benchmark(), MemoryMonitoring() and Analytics() classes.
    """

    def __init__(self, *args):
        """
        Init function.

        :param args:
        """

        pass
