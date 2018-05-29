#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# Structure of pilot timing dictionary:
# { job_id: { <timing_constant_1>: <time measurement in seconds since epoch>, .. }
# job_id = 0 means timing information from wrapper. Timing constants are defined in pilot.util.constants.
# Time measurement are time.time() values.

import logging
logger = logging.getLogger(__name__)
