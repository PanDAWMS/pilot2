#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

SUCCESS = 0
FAILURE = 1

ERRNO_NOJOBS = 20

# Sorting order constants
UTILITY_BEFORE_PAYLOAD = 0
UTILITY_WITH_PAYLOAD = 1
UTILITY_AFTER_PAYLOAD = 2
UTILITY_WITH_STAGEIN = 3
