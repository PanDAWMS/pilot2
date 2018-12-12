#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

# This script shows how to use the Data API stage-in client to download a file from storage.

from pilot.api import data
from pilot.info import FileSpec

client = data.StageInClient()
files = [{'scope': 'mc16_13TeV', 'lfn': 'EVNT.11320990._003958.pool.root.1', 'workdir': '.',
          'ddmendpoint': 'RRC-KI-T1_DATADISK'}]
xfiles = [FileSpec(type='input', **f) for f in files]
r = client.transfer(xfiles)
