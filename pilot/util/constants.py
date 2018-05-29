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

# Timing constants. Values might be used for sorting purposes. Values have been chosen to be future proof, to allow for
# additional constants to be added if necessary. They also allow for timing constants to be defined for values before
# the pilot is started, ie for wrapper timing purposes.
PILOT_T0 = 100
PILOT_PRE_GETJOB = 110
PILOT_POST_GETJOB = 112
PILOT_PRE_SETUP = 120
PILOT_POST_SETUP = 122
PILOT_PRE_STAGEIN = 130
PILOT_POST_STAGEIN = 132
PILOT_PRE_PAYLOAD = 140
PILOT_POST_PAYLOAD = 142
PILOT_PRE_STAGEOUT = 150
PILOT_POST_STAGEOUT = 152
PILOT_PRE_FINAL_UPDATE = 160
PILOT_POST_FINAL_UPDATE = 162
PILOT_END_TIME = 199
