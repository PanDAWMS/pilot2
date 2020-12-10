#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

import os
import time
import signal
import re
import threading

from pilot.util.container import execute
from pilot.util.auxiliary import whoami
from pilot.util.filehandling import read_file, remove_dir_tree

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

    if not pid:
        return

    cpids.append(pid)

    cmd = "ps -eo pid,ppid -m | grep %d" % pid
    exit_code, psout, stderr = execute(cmd, mute=True)

    lines = psout.split("\n")
    if lines != ['']:
        for i in range(0, len(lines)):
            try:
                thispid = int(lines[i].split()[0])
                thisppid = int(lines[i].split()[1])
            except Exception as e:
                logger.warning('exception caught: %s' % e)
            if thisppid == pid:
                find_processes_in_group(cpids, thispid)


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


def kill_processes(pid):
    """
    Kill process beloging to given process group.

    :param pid: process id (int).
    :return:
    """

    # if there is a known subprocess pgrp, then it should be enough to kill the group in one go
    status = False
    try:
        pgrp = os.getpgid(pid)
    except Exception as e:
        pgrp = 0
    if pgrp != 0:
        status = kill_process_group(pgrp)

    if not status:
        # firstly find all the children process IDs to be killed
        children = []
        find_processes_in_group(children, pid)

        # reverse the process order so that the athena process is killed first (otherwise the stdout will be truncated)
        if not children:
            return

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
                    kill_process(i)

    # kill any remaining orphan processes
    kill_orphans()


def kill_child_processes(pid):
    """
    Kill child processes.

    :param pid: process id (int).
    :return:
    """
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
                kill_process(i)


def kill_process_group(pgrp):
    """
    Kill the process group.

    :param pgrp: process group id (int).
    :return: boolean (True if SIGMTERM followed by SIGKILL signalling was successful)
    """

    status = False
    _sleep = True

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

    return status


def kill_process(pid):
    """
    Kill process.

    :param pid: process id (int).
    :return: boolean (True if successful SIGKILL)
    """

    status = False

    try:
        os.kill(pid, signal.SIGTERM)
    except Exception as e:
        logger.warning("exception thrown when killing child process %d with SIGTERM: %s" % (pid, e))
    else:
        logger.info("killed process %d with SIGTERM" % pid)

    _t = 10
    logger.info("sleeping %d s to allow process to exit" % _t)
    time.sleep(_t)

    # now do a hard kill just in case some processes haven't gone away
    try:
        os.kill(pid, signal.SIGKILL)
    except Exception as e:
        logger.warning("exception thrown when killing child process %d with SIGKILL,"
                       "ignore this if it was already killed by previous SIGTERM: %s" % (pid, e))
    else:
        logger.info("killed process %d with SIGKILL" % pid)
        status = True

    return status


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
        if pid:
            n = len(children)
            logger.info("number of running child processes to parent process %d: %d" % (pid, n))
        else:
            logger.debug("pid not yet set")
    return n


def kill_orphans():
    """
    Find and kill all orphan processes belonging to current pilot user.

    :return:
    """

    # exception for BOINC
    if 'BOINC' in os.environ.get('PILOT_SITENAME', ''):
        logger.info("Do not look for orphan processes in BOINC jobs")
        return

    if 'PILOT_NOKILL' in os.environ:
        return

    logger.info("searching for orphan processes")

    cmd = "ps -o pid,ppid,args -u %s" % whoami()
    exit_code, _processes, stderr = execute(cmd)
    #pattern = re.compile(r'(\d+)\s+(\d+)\s+(\S+)')  # Python 3 (added r)
    pattern = re.compile(r'(\d+)\s+(\d+)\s+([\S\s]+)')  # Python 3 (added r)

    count = 0
    for line in _processes.split('\n'):
        ids = pattern.search(line)
        if ids:
            pid = ids.group(1)
            ppid = ids.group(2)
            args = ids.group(3)
            if 'cvmfs2' in args:
                logger.info("ignoring possible orphan process running cvmfs2: pid=%s, ppid=%s, args=\'%s\'" %
                            (pid, ppid, args))
            elif 'pilots_starter.py' in args:
                logger.info("ignoring pilot launcher: pid=%s, ppid=%s, args='%s'" % (pid, ppid, args))
            elif ppid == '1':
                count += 1
                logger.info("found orphan process: pid=%s, ppid=%s, args='%s'" % (pid, ppid, args))
                #if args.endswith('bash'):
                if 'bash' in args:
                    logger.info("will not kill bash process")
                else:
                    try:
                        os.killpg(int(pid), signal.SIGKILL)
                    except Exception as e:
                        logger.warning("failed to execute killpg(): %s" % e)
                        cmd = 'kill -9 %s' % (pid)
                        exit_code, rs, stderr = execute(cmd)
                        if exit_code != 0:
                            logger.warning(rs)
                        else:
                            logger.info("killed orphaned process %s (%s)" % (pid, args))
                    else:
                        logger.info("killed orphaned process group %s (%s)" % (pid, args))

    if count == 0:
        logger.info("did not find any orphan processes")
    else:
        logger.info("found %d orphan process(es)" % count)


def get_max_memory_usage_from_cgroups():
    """
    Read the max_memory from CGROUPS file memory.max_usage_in_bytes.

    :return: max_memory (int).
    """

    max_memory = None

    # Get the CGroups max memory using the pilot pid
    pid = os.getpid()
    path = "/proc/%d/cgroup" % pid
    if os.path.exists(path):
        cmd = "grep memory %s" % path
        exit_code, out, stderr = execute(cmd)
        if out == "":
            logger.info("(command did not return anything)")
        else:
            logger.info(out)
            if ":memory:" in out:
                pos = out.find('/')
                path = out[pos:]
                logger.info("extracted path = %s" % path)

                pre = get_cgroups_base_path()
                if pre != "":
                    path = pre + os.path.join(path, "memory.max_usage_in_bytes")
                    logger.info("path to CGROUPS memory info: %s" % path)
                    max_memory = read_file(path)
                else:
                    logger.info("CGROUPS base path could not be extracted - not a CGROUPS site")
            else:
                logger.warning("invalid format: %s (expected ..:memory:[path])" % out)
    else:
        logger.info("path %s does not exist (not a CGROUPS site)" % path)

    return max_memory


def get_cgroups_base_path():
    """
    Return the base path for CGROUPS.

    :return: base path for CGROUPS (string).
    """

    cmd = "grep \'^cgroup\' /proc/mounts|grep memory| awk \'{print $2}\'"
    exit_code, base_path, stderr = execute(cmd, mute=True)

    return base_path


def get_cpu_consumption_time(t0):
    """
    Return the CPU consumption time for child processes measured by system+user time from os.times().
    Note: the os.times() tuple is user time, system time, s user time, s system time, and elapsed real time since a
    fixed point in the past.

    :param t0: initial os.times() tuple prior to measurement.
    :return: system+user time for child processes (float).
    """

    t1 = os.times()
    user_time = t1[2] - t0[2]
    system_time = t1[3] - t0[3]

    return user_time + system_time


def get_instant_cpu_consumption_time(pid):
    """
    Return the CPU consumption time (system+user time) for a given process, by parsing /prod/pid/stat.
    Note 1: the function returns 0.0 if the pid is not set.
    Note 2: the function must sum up all the user+system times for both the main process (pid) and the child
    processes, since the main process is most likely spawning new processes.

    :param pid: process id (int).
    :return: system+user time for a given pid (float).
    """

    utime = None
    stime = None
    cutime = None
    cstime = None

    hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
    if type(hz) != int:
        logger.warning('unknown SC_CLK_TCK: %s' % str(hz))
        return 0.0

    if pid and hz and hz > 0:
        path = "/proc/%d/stat" % pid
        if os.path.exists(path):
            with open(path) as fp:
                fields = fp.read().split(' ')[13:17]
                utime, stime, cutime, cstime = [(float(f) / hz) for f in fields]

    if utime and stime and cutime and cstime:
        # sum up all the user+system times for both the main process (pid) and the child processes
        cpu_consumption_time = utime + stime + cutime + cstime
    else:
        cpu_consumption_time = 0.0

    return cpu_consumption_time


def get_current_cpu_consumption_time(pid):
    """
    Get the current CPU consumption time (system+user time) for a given process, by looping over all child processes.

    :param pid: process id (int).
    :return: system+user time for a given pid (float).
    """

    # get all the child processes
    children = []
    find_processes_in_group(children, pid)

    cpuconsumptiontime = 0
    for _pid in children:
        _cpuconsumptiontime = get_instant_cpu_consumption_time(_pid)
        if _cpuconsumptiontime:
            cpuconsumptiontime += _cpuconsumptiontime

    return cpuconsumptiontime


def is_process_running(process_id):
    """
    Check whether process is still running.

    :param process_id: process id (int).
    :return: Boolean.
    """
    try:
        # note that this kill function call will not kill the process
        os.kill(process_id, 0)
        return True
    except OSError:
        return False


def cleanup(job, args):
    """
    Cleanup called after completion of job.

    :param job: job object
    :return:
    """

    logger.info("overall cleanup function is called")

    # make sure the workdir is deleted
    if args.cleanup:
        if remove_dir_tree(job.workdir):
            logger.info('removed %s' % job.workdir)

        if os.path.exists(job.workdir):
            logger.warning('work directory still exists: %s' % job.workdir)
        else:
            logger.debug('work directory was removed: %s' % job.workdir)
    else:
        logger.info('workdir not removed %s' % job.workdir)

    # collect any zombie processes
    job.collect_zombies(tn=10)
    logger.info("collected zombie processes")

    if job.pid:
        logger.info("will now attempt to kill all subprocesses of pid=%d" % job.pid)
        kill_processes(job.pid)
    else:
        logger.warning('cannot kill any subprocesses since job.pid is not set')
    #logger.info("deleting job object")
    #del job


def threads_aborted(abort_at=2):
    """
    Have the threads been aborted?

    :param abort_at: 1 for workflow finish, 2 for thread finish (since check is done just before thread finishes) (int).
    :return: Boolean.
    """

    aborted = False
    thread_count = threading.activeCount()

    # count all non-daemon threads
    daemon_threads = 0
    for thread in threading.enumerate():
        if thread.isDaemon():  # ignore any daemon threads, they will be aborted when python ends
            daemon_threads += 1

    if thread_count - daemon_threads == abort_at:
        logger.debug('aborting since the last relevant thread is about to finish')
        aborted = True

    return aborted
