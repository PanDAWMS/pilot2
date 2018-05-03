#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import os
import time
import signal

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
    :return: (updated cpids input parameter list).
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
            except Exception:
                pass
            if thisppid == pid:
                findProcessesInGroup(cpids, thispid)


def is_zombie(pid):
    """
    Is the given process a zombie?
    :param pid: process id (int).
    :return: boolean.
    """

    status = False

    cmd = "ps aux | grep %d" % (pid)
    exit_code, stdout, stderr = execute(cmd, mute=True)
    if "<defunct>" in stdout:
        status = True

    return status


def get_process_commands(euid, pids):
    """
    Return a list of process commands corresponding to a pid list for user euid.

    :param euid: user id (int).
    :param pids: list of process id's.
    :return: list of process commands.
    """

    cmd = 'ps u -u %d' % euid
    process_commands = []
    exit_code, stdout, stderr = execute(cmd, mute=True)

    if exit_code != 0 or stdout == '':
        logger.warning('ps command failed: %d, \"%s\", \"%s\"' % (exit_code, stdout, stderr))
    else:
        # extract the relevant processes
        p_commands = stdout.split('\n')
        first = True
        for p_command in p_commands:
            if first:
                # get the header info line
                process_commands.append(p_command)
                first = False
            else:
                # remove extra spaces
                _p_command = p_command
                while "  " in _p_command:
                    _p_command = _p_command.replace("  ", " ")
                items = _p_command.split(" ")
                for pid in pids:
                    # items = username pid ...
                    if items[1] == str(pid):
                        process_commands.append(p_command)
                        break

    return process_commands


def dump_stack_trace(pid):
    """
    Execute the stack trace command (pstack <pid>).

    :param pid: process id (int).
    :return:
    """

    # make sure that the process is not in a zombie state
    if not is_zombie(pid):
        cmd = "pstack %d" % (pid)
        exit_code, stdout, stderr = execute(cmd, mute=True, timeout=60)
        logger.info(stdout or "(pstack returned empty string)")
    else:
        logger.info("skipping pstack dump for zombie process")


def kill_processes(pid, pgrp):
    """
    Kill process beloging to given process group.

    :param pid: process id (int).
    :param pgrp: process group (int).
    :return:
    """

    # if there is a known subprocess pgrp, then it should be enough to kill the group in one go
    status = False
    _sleep = True

    if pgrp != 0:
        # kill the process gracefully
        logger.info("killing group process %d" % pgrp)
        try:
            os.killpg(pgrp, signal.SIGTERM)
        except Exception as e:
            logger.warning("exception thrown when killing child group process under SIGTERM: %s" % e)
            _sleep = False
        else:
            logger.info("SIGTERM sent to process group %d" % pgrp)

        if _sleep:
            _t = 30
            logger.info("sleeping %d s to allow processes to exit" % _t)
            time.sleep(_t)

        try:
            os.killpg(pgrp, signal.SIGKILL)
        except Exception as e:
            logger.warning("exception thrown when killing child group process with SIGKILL: %s" % e)
        else:
            logger.info("SIGKILL sent to process group %d" % pgrp)
            status = True

    if not status:
        # firstly find all the children process IDs to be killed
        children = []
        find_processes_in_group(children, pid)

        # reverse the process order so that the athena process is killed first (otherwise the stdout will be truncated)
        children.reverse()
        logger.info("process IDs to be killed: %s (in reverse order)" % str(children))

        # find which commands are still running
        try:
            cmds = get_process_commands(os.geteuid(), children)
        except Exception as e:
            logger.warning("get_process_commands() threw an exception: %s" % e)
        else:
            if len(cmds) <= 1:
                logger.warning("found no corresponding commands to process id(s)")
            else:
                logger.info("found commands still running:")
                for cmd in cmds:
                    logger.info(cmd)

                # loop over all child processes
                for i in children:
                    # dump the stack trace before killing it
                    dump_stack_trace(i)

                    # kill the process gracefully
                    try:
                        os.kill(i, signal.SIGTERM)
                    except Exception as e:
                        logger.warning("exception thrown when killing child process %d with SIGTERM: %s" % (i, e))
                        pass
                    else:
                        logger.info("killed process %d with SIGTERM" % i)

                    _t = 10
                    logger.info("sleeping %d s to allow process to exit" % _t)
                    time.sleep(_t)

                    # now do a hard kill just in case some processes haven't gone away
                    try:
                        os.kill(i, signal.SIGKILL)
                    except Exception as e:
                        logger.warning("exception thrown when killing child process %d with SIGKILL,"
                                       "ignore this if it was already killed by previous SIGTERM: %s" % (i, e))
                    else:
                        logger.info("killed process %d with SIGKILL" % i)

    # kill any remaining orphan processes
    kill_orphans()


# called checkProcesses() in Pilot 1, used by process monitoring
def get_number_of_child_processes(pid):
    """
    Get the number of child processes for a given parent process.

    :param pid: parent process id (int).
    :return: number of child processes (int).
    """

    children = []
    n = 0
    try:
        find_processes_in_group(children, pid)
    except Exception as e:
        logger.warning("exception caught in find_processes_in_group: %s" % e)
    else:
        n = len(children)
        logger.info("number of running child processes to parent process %d: %d" % (pid, n))

    return n
