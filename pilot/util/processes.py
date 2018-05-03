#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def find_processes_in_group(cpids, pid):
    """
    Find all processes that belong to the same group.
    Recursively search for the children processes belonging to pid and return their pid's.
    pid is the parent pid and cpids is a list that has to be initialized before calling this function and it contains
    the pids of the children AND the parent.

    :param cpids: list of pid's for all child processes to the parent pid, as well as the parent pid itself (int).
    :param pid: parent process id (int).
    :return:
    """

    cpids.append(pid)
    cmd = "ps -eo pid,ppid -m | grep %d" % pid
    exit_code, psout, stderr = execute(cmd, mute=True)

    lines = psout.split("\n")
    if lines != ['']:
        for i in range(0, len(lines)):
            try:
                thispid = int(lines[i].split()[0])
                thisppid = int(lines[i].split()[1])
            except ValueError:
                pass
            if thisppid == pid:
                findProcessesInGroup(cpids, thispid)
