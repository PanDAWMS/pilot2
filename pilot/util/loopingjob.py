#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018


def killer():
    """
    Looping job killer algorithm.
    Identify hanging tasks/processes. Did the stage-in/out finish within allowed time limit, or did the payload update
    any files recently?

    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    return exit_code, diagnostics
