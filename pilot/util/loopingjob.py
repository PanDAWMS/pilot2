#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018


def looping_job(job):
    """
    Looping job detection algorithm.
    Identify hanging tasks/processes. Did the stage-in/out finish within allowed time limit, or did the payload update
    any files recently?

    :param job: job object.
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    if job.state == 'stagein':
        # set job.state to stagein during stage-in before implementing this algorithm
        pass
    elif job.state == 'stageout':
        # set job.state to stageout during stage-out before implementing this algorithm
        pass
    else:
        # most likely in the 'running' state, but use the catch-all 'else'
        pass

    return exit_code, diagnostics
