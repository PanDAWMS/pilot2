#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2018
# - Wen Guan, wen.guan@cern.ch, 2018

SUCCESS = 0
FAILURE = 1

ERRNO_NOJOBS = 20

# Sorting order constants
UTILITY_BEFORE_PAYLOAD = 0
UTILITY_WITH_PAYLOAD = 1
UTILITY_AFTER_PAYLOAD_STARTED = 2
UTILITY_AFTER_PAYLOAD_FINISHED = 3
UTILITY_WITH_STAGEIN = 4

# Timing constants that allow for additional constants to be defined for values before the pilot is started, ie for
# wrapper timing purposes.
PILOT_T0 = 'PILOT_T0'
PILOT_PRE_GETJOB = 'PILOT_PRE_GETJOB'
PILOT_POST_GETJOB = 'PILOT_POST_GETJOB'  # note: PILOT_POST_GETJOB corresponds to START_TIME in Pilot 1
PILOT_PRE_SETUP = 'PILOT_PRE_SETUP'
PILOT_POST_SETUP = 'PILOT_POST_SETUP'
PILOT_PRE_STAGEIN = 'PILOT_PRE_STAGEIN'
PILOT_POST_STAGEIN = 'PILOT_POST_STAGEIN'
PILOT_PRE_PAYLOAD = 'PILOT_PRE_PAYLOAD'
PILOT_POST_PAYLOAD = 'PILOT_POST_PAYLOAD'
PILOT_PRE_STAGEOUT = 'PILOT_PRE_STAGEOUT'
PILOT_POST_STAGEOUT = 'PILOT_POST_STAGEOUT'
PILOT_PRE_FINAL_UPDATE = 'PILOT_PRE_FINAL_UPDATE'
PILOT_POST_FINAL_UPDATE = 'PILOT_POST_FINAL_UPDATE'
PILOT_END_TIME = 'PILOT_END_TIME'
