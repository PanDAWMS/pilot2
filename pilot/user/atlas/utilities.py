#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2020

import os
import time
from re import search

# from pilot.info import infosys
from .setup import get_asetup
from pilot.util.auxiliary import is_python3
from pilot.util.container import execute
from pilot.util.filehandling import read_json, copy, write_json, remove
from pilot.util.parameters import convert_to_int
from pilot.util.processes import is_process_running

import logging
logger = logging.getLogger(__name__)


def get_benchmark_setup(job):
    """
    Return the proper setup for the benchmark command.

    :param job: job object.
    :return: setup string for the benchmark command.
    """

    return ''


def get_prefetcher_setup(job):
    """
    Return the proper setup for the Prefetcher.
    Prefetcher is a tool used with the Event Streaming Service.

    :param job: job object.
    :return: setup string for the Prefetcher command.
    """

    # add code here ..

    return ''


def get_network_monitor_setup(setup, job):
    """
    Return the proper setup for the network monitor.
    The network monitor is currently setup together with the payload and is start before it. The payload setup should
    therefore be provided. The network monitor setup is prepended to it.

    :param setup: payload setup string.
    :param job: job object.
    :return: network monitor setup string.
    """

    return ''


def get_memory_monitor_summary_filename(selector=None):
    """
    Return the name for the memory monitor summary file.

    :param selector: special conditions flag (boolean).
    :return: File name (string).
    """

    name = "memory_monitor_summary.json"
    if selector:
        name += '_snapshot'

    return name


def get_memory_monitor_output_filename(suffix='txt'):
    """
    Return the filename of the memory monitor text output file.

    :return: File name (string).
    """

    return "memory_monitor_output.%s" % suffix


def get_memory_monitor_setup(pid, pgrp, jobid, workdir, command, setup="", use_container=True, transformation="", outdata=None, dump_ps=False):
    """
    Return the proper setup for the memory monitor.
    If the payload release is provided, the memory monitor can be setup with the same release. Until early 2018, the
    memory monitor was still located in the release area. After many problems with the memory monitor, it was decided
    to use a fixed version for the setup. Currently, release 21.0.22 is used.

    :param pid: job process id (int).
    :param pgrp: process group id (int).
    :param jobid: job id (int).
    :param workdir: job work directory (string).
    :param command: payload command (string).
    :param setup: optional setup in case asetup can not be used, which uses infosys (string).
    :param use_container: optional boolean.
    :param transformation: optional name of transformation, e.g. Sim_tf.py (string).
    :param outdata: optional list of output fspec objects (list).
    :param dump_ps: should ps output be dumped when identifying prmon process? (Boolean).
    :return: job work directory (string), pid for process inside container (int).
    """

    # try to get the pid from a pid.txt file which might be created by a container_script
    pid = get_proper_pid(pid, pgrp, jobid, command=command, transformation=transformation, outdata=outdata, use_container=use_container, dump_ps=dump_ps)
    if pid == -1:
        logger.warning('process id was not identified before payload finished - will not launch memory monitor')
        return "", pid

    if not setup:
        setup = get_asetup(asetup=False)
        setup += 'lsetup prmon;'
    if not setup.endswith(';'):
        setup += ';'

    cmd = "prmon"
    interval = 60
    options = " --pid %d --filename %s --json-summary %s --interval %d" %\
              (pid, get_memory_monitor_output_filename(), get_memory_monitor_summary_filename(), interval)
    cmd = "cd " + workdir + ";" + setup + cmd + options

    return cmd, pid


def get_memory_monitor_setup_old(pid, pgrp, jobid, workdir, command, setup="", use_container=True, transformation="", outdata=None, dump_ps=False):
    """
    Return the proper setup for the memory monitor.
    If the payload release is provided, the memory monitor can be setup with the same release. Until early 2018, the
    memory monitor was still located in the release area. After many problems with the memory monitor, it was decided
    to use a fixed version for the setup. Currently, release 21.0.22 is used.

    :param pid: job process id (int).
    :param pgrp: process group id (int).
    :param jobid: job id (int).
    :param workdir: job work directory (string).
    :param command: payload command (string).
    :param setup: optional setup in case asetup can not be used, which uses infosys (string).
    :param use_container: optional boolean.
    :param transformation: optional name of transformation, e.g. Sim_tf.py (string).
    :param outdata: optional list of output fspec objects (list).
    :param dump_ps: should ps output be dumped when identifying prmon process? (Boolean).
    :return: job work directory (string), pid for process inside container (int).
    """

    # try to get the pid from a pid.txt file which might be created by a container_script
    pid = get_proper_pid(pid, pgrp, jobid, command=command, transformation=transformation, outdata=outdata, use_container=use_container, dump_ps=dump_ps)
    if pid == -1:
        logger.warning('process id was not identified before payload finished - will not launch memory monitor')
        return "", pid

    release = "22.0.1"
    platform = "x86_64-centos7-gcc8-opt"
    if not setup:
        setup = get_asetup() + " Athena," + release + " --platform " + platform
    interval = 60
    if not setup.endswith(';'):
        setup += ';'
    # Decide which version of the memory monitor should be used
    cmd = "%swhich prmon" % setup
    exit_code, stdout, stderr = execute(cmd)
    if stdout and "Command not found" not in stdout:
        _cmd = "prmon "
    else:
        logger.warning('failed to find prmon, defaulting to old memory monitor: %d, %s' % (exit_code, stderr))
        _cmd = "MemoryMonitor "
        setup = setup.replace(release, "21.0.22")
        setup = setup.replace(platform, "x86_64-slc6-gcc62-opt")

    options = "--pid %d --filename %s --json-summary %s --interval %d" %\
              (pid, get_memory_monitor_output_filename(), get_memory_monitor_summary_filename(), interval)
    _cmd = "cd " + workdir + ";" + setup + _cmd + options

    return _cmd, pid


def get_proper_pid(pid, pgrp, jobid, command="", transformation="", outdata="", use_container=True, dump_ps=False):
    """
    Return a pid from the proper source to be used with the memory monitor.
    The given pid comes from Popen(), but in the case containers are used, the pid should instead come from a ps aux
    lookup.
    If the main process has finished before the proper pid has been identified (it will take time if the payload is
    running inside a container), then this function will abort and return -1. The called should handle this and not
    launch the memory monitor as it is not needed any longer.

    :param pid: process id (int).
    :param pgrp: process group id (int).
    :param jobid: job id (int).
    :param command: payload command (string).
    :param transformation: optional name of transformation, e.g. Sim_tf.py (string).
    :param outdata: list of output fspec object (list).
    :param use_container: optional boolean.
    :return: pid (int).
    """

    if not use_container:
        return pid

    # abort if main process has finished already
    if not is_process_running(pid):
        return -1

    #_cmd = get_trf_command(command, transformation=transformation)
    # get ps info using group id
    ps = get_ps_info(pgrp)
    if dump_ps:
        logger.debug('ps:\n%s' % ps)
    #logger.debug('attempting to identify pid for Singularity (v.3) runtime parent process')
    #_pid = get_pid_for_command(ps, command="Singularity runtime parent")
    #if _pid:
    #    logger.debug('discovered pid=%d for process \"%s\"' % (_pid, _cmd))
    #    return _pid

    i = 0
    imax = 120
    while i < imax:
        # abort if main process has finished already
        if not is_process_running(pid):
            return -1

        ps = get_ps_info(pgrp)
        #logger.debug('ps:\n%s' % ps)

        # lookup the process id using ps aux
        logger.debug('attempting to identify pid from job id')
        _pid = get_pid_for_jobid(ps, jobid)
        if _pid:
            logger.debug('discovered pid=%d for job id %s' % (_pid, jobid))
            break

        #logger.debug('attempting to identify pid from transform name and its output')
        #_pid = get_pid_for_trf(ps, transformation, outdata) if outdata else None
        #if _pid:
        #    logger.debug('discovered pid=%d for transform name \"%s\"' % (_pid, transformation))
        #    break

        logger.warning('payload pid has not yet been identified (#%d/#%d)' % (i + 1, imax))

        # wait until the payload has launched
        time.sleep(5)
        i += 1

    if _pid:
        pid = _pid

    logger.info('will use pid=%d for memory monitor' % pid)

    return pid


def get_ps_info(pgrp, whoami=None, options='axfo pid,user,args'):
    """
    Return ps info for the given user.

    :param pgrp: process group id (int).
    :param whoami: user name (string).
    :return: ps aux for given user (string).
    """

    if not whoami:
        whoami = os.getuid()

    cmd = "ps -u %s %s" % (whoami, options)
    #cmd = "ps %s | grep %s" % (options, whoami)
    #cmd = "ps %s | grep %s | awk -v p=%s '$1 == p {print $5}" % (options, whoami, pgrp)
    #cmd = "ps %s | awk -v p=%s '$1 == p {print $5}" % (options, pgrp)
    exit_code, stdout, stderr = execute(cmd)

    return stdout


def get_pid_for_jobid(ps, jobid):
    """
    Return the process id for the ps entry that contains the job id.

    :param ps: ps command output (string).
    :param jobid: PanDA job id (int).
    :return: pid (int) or None if no such process.
    """

    pid = None

    for line in ps.split('\n'):
        if jobid in line:
            # extract pid
            _pid = search(r'(\d+) ', line)
            try:
                pid = int(_pid.group(1))
            except Exception as e:
                logger.warning('pid has wrong type: %s' % e)
            else:
                logger.debug('extracted pid=%d from ps output' % pid)
            break

    return pid


def get_pid_for_trf(ps, transformation, outdata):
    """
    Return the process id for the given command and user.
    Note: function returns 0 in case pid could not be found.

    :param ps: ps command output (string).
    :param transformation: transformation name, e.g. Sim_tf.py (String).
    :param outdata: fspec objects (list).
    :return: pid (int) or None if no such process.
    """

    pid = None
    candidates = []

    # in the case of user analysis job, the transformation will contain a URL which should be stripped
    if "/" in transformation:
        transformation = transformation.split('/')[-1]
    logger.debug('using transformation name: %s' % transformation)
    for line in ps.split('\n'):
        if transformation in line:
            candidates.append(line)
            break

    if candidates:
        for line in candidates:
            for fspec in outdata:
                if fspec.lfn in line:
                    # extract pid
                    _pid = search(r'(\d+) ', line)
                    try:
                        pid = int(_pid.group(1))
                    except Exception as e:
                        logger.warning('pid has wrong type: %s' % e)
                    else:
                        logger.debug('extracted pid=%d from ps output' % pid)
                    break
            if pid:
                break
    else:
        logger.debug('pid not found in ps output for trf=%s' % transformation)

    return pid


def get_pid_for_command(ps, command="python pilot2/pilot.py"):
    """
    Return the process id for the given command and user.
    The function returns 0 in case pid could not be found.
    If no command is specified, the function looks for the "python pilot2/pilot.py" command in the ps output.

    :param ps: ps command output (string).
    :param command: command string expected to be in ps output (string).
    :return: pid (int) or None if no such process.
    """

    pid = None
    found = None

    for line in ps.split('\n'):
        if command in line:
            found = line
            break
    if found:
        # extract pid
        _pid = search(r'(\d+) ', found)
        try:
            pid = int(_pid.group(1))
        except Exception as e:
            logger.warning('pid has wrong type: %s' % e)
        else:
            logger.debug('extracted pid=%d from ps output: %s' % (pid, found))
    else:
        logger.debug('command not found in ps output: %s' % command)

    return pid


def get_trf_command(command, transformation=""):
    """
    Return the last command in the full payload command string.
    Note: this function returns the last command in job.command which is only set for containers.

    :param command: full payload command (string).
    :param transformation: optional name of transformation, e.g. Sim_tf.py (string).
    :return: trf command (string).
    """

    payload_command = ""
    if command:
        if not transformation:
            payload_command = command.split(';')[-2]
        else:
            if transformation in command:
                payload_command = command[command.find(transformation):]

        # clean-up the command, remove '-signs and any trailing ;
        payload_command = payload_command.strip()
        payload_command = payload_command.replace("'", "")
        payload_command = payload_command.rstrip(";")

    return payload_command


def get_memory_monitor_info_path(workdir, allowtxtfile=False):
    """
    Find the proper path to the utility info file
    Priority order:
       1. JSON summary file from workdir
       2. JSON summary file from pilot initdir
       3. Text output file from workdir (if allowtxtfile is True)

    :param workdir: relevant work directory (string).
    :param allowtxtfile: boolean attribute to allow for reading the raw memory monitor output.
    :return: path (string).
    """

    pilot_initdir = os.environ.get('PILOT_HOME', '')
    path = os.path.join(workdir, get_memory_monitor_summary_filename())
    init_path = os.path.join(pilot_initdir, get_memory_monitor_summary_filename())

    if not os.path.exists(path):
        if os.path.exists(init_path):
            path = init_path
        else:
            logger.info("neither %s, nor %s exist" % (path, init_path))
            path = ""

        if path == "" and allowtxtfile:
            path = os.path.join(workdir, get_memory_monitor_output_filename())
            if not os.path.exists(path):
                logger.warning("file does not exist either: %s" % (path))

    return path


def get_memory_monitor_info(workdir, allowtxtfile=False, name=""):  # noqa: C901
    """
    Add the utility info to the node structure if available.

    :param workdir: relevant work directory (string).
    :param allowtxtfile: boolean attribute to allow for reading the raw memory monitor output.
    :param name: name of memory monitor (string).
    :return: node structure (dictionary).
    """

    node = {}

    # Get the values from the memory monitor file (json if it exists, otherwise the preliminary txt file)
    # Note that only the final json file will contain the totRBYTES, etc
    try:
        summary_dictionary = get_memory_values(workdir, name=name)
    except Exception as e:
        logger.warning('failed to get memory values from memory monitor tool: %s' % e)
        summary_dictionary = {}
    else:
        logger.debug("summary_dictionary=%s" % str(summary_dictionary))

    # Fill the node dictionary
    if summary_dictionary and summary_dictionary != {}:
        # first determine which memory monitor version was running (MemoryMonitor or prmon)
        if 'maxRSS' in summary_dictionary['Max']:
            version = 'MemoryMonitor'
        elif 'rss' in summary_dictionary['Max']:
            version = 'prmon'
        else:
            version = 'unknown'
        if version == 'MemoryMonitor':
            try:
                node['maxRSS'] = summary_dictionary['Max']['maxRSS']
                node['maxVMEM'] = summary_dictionary['Max']['maxVMEM']
                node['maxSWAP'] = summary_dictionary['Max']['maxSwap']
                node['maxPSS'] = summary_dictionary['Max']['maxPSS']
                node['avgRSS'] = summary_dictionary['Avg']['avgRSS']
                node['avgVMEM'] = summary_dictionary['Avg']['avgVMEM']
                node['avgSWAP'] = summary_dictionary['Avg']['avgSwap']
                node['avgPSS'] = summary_dictionary['Avg']['avgPSS']
            except Exception as e:
                logger.warning("exception caught while parsing memory monitor file: %s" % e)
                logger.warning("will add -1 values for the memory info")
                node['maxRSS'] = -1
                node['maxVMEM'] = -1
                node['maxSWAP'] = -1
                node['maxPSS'] = -1
                node['avgRSS'] = -1
                node['avgVMEM'] = -1
                node['avgSWAP'] = -1
                node['avgPSS'] = -1
            else:
                logger.info("extracted standard info from memory monitor json")
            try:
                node['totRCHAR'] = summary_dictionary['Max']['totRCHAR']
                node['totWCHAR'] = summary_dictionary['Max']['totWCHAR']
                node['totRBYTES'] = summary_dictionary['Max']['totRBYTES']
                node['totWBYTES'] = summary_dictionary['Max']['totWBYTES']
                node['rateRCHAR'] = summary_dictionary['Avg']['rateRCHAR']
                node['rateWCHAR'] = summary_dictionary['Avg']['rateWCHAR']
                node['rateRBYTES'] = summary_dictionary['Avg']['rateRBYTES']
                node['rateWBYTES'] = summary_dictionary['Avg']['rateWBYTES']
            except Exception:
                logger.warning("standard memory fields were not found in memory monitor json (or json doesn't exist yet)")
            else:
                logger.info("extracted standard memory fields from memory monitor json")
        elif version == 'prmon':
            try:
                node['maxRSS'] = int(summary_dictionary['Max']['rss'])
                node['maxVMEM'] = int(summary_dictionary['Max']['vmem'])
                node['maxSWAP'] = int(summary_dictionary['Max']['swap'])
                node['maxPSS'] = int(summary_dictionary['Max']['pss'])
                node['avgRSS'] = summary_dictionary['Avg']['rss']
                node['avgVMEM'] = summary_dictionary['Avg']['vmem']
                node['avgSWAP'] = summary_dictionary['Avg']['swap']
                node['avgPSS'] = summary_dictionary['Avg']['pss']
            except Exception as e:
                logger.warning("exception caught while parsing prmon file: %s" % e)
                logger.warning("will add -1 values for the memory info")
                node['maxRSS'] = -1
                node['maxVMEM'] = -1
                node['maxSWAP'] = -1
                node['maxPSS'] = -1
                node['avgRSS'] = -1
                node['avgVMEM'] = -1
                node['avgSWAP'] = -1
                node['avgPSS'] = -1
            else:
                logger.info("extracted standard info from prmon json")
            try:
                node['totRCHAR'] = int(summary_dictionary['Max']['rchar'])
                node['totWCHAR'] = int(summary_dictionary['Max']['wchar'])
                node['totRBYTES'] = int(summary_dictionary['Max']['read_bytes'])
                node['totWBYTES'] = int(summary_dictionary['Max']['write_bytes'])
                node['rateRCHAR'] = summary_dictionary['Avg']['rchar']
                node['rateWCHAR'] = summary_dictionary['Avg']['wchar']
                node['rateRBYTES'] = summary_dictionary['Avg']['read_bytes']
                node['rateWBYTES'] = summary_dictionary['Avg']['write_bytes']
            except Exception:
                logger.warning("standard memory fields were not found in prmon json (or json doesn't exist yet)")
            else:
                logger.info("extracted standard memory fields from prmon json")
        else:
            logger.warning('unknown memory monitor version')
    else:
        logger.info("memory summary dictionary not yet available")

    return node


def get_max_memory_monitor_value(value, maxvalue, totalvalue):  # noqa: C90
    """
    Return the max and total value (used by memory monitoring).
    Return an error code, 1, in case of value error.

    :param value: value to be tested (integer).
    :param maxvalue: current maximum value (integer).
    :param totalvalue: total value (integer).
    :return: exit code, maximum and total value (tuple of integers).
    """

    ec = 0
    try:
        value_int = int(value)
    except Exception as e:
        logger.warning("exception caught: %s" % e)
        ec = 1
    else:
        totalvalue += value_int
        if value_int > maxvalue:
            maxvalue = value_int

    return ec, maxvalue, totalvalue


def convert_unicode_string(unicode_string):
    """
    Convert a unicode string into str.

    :param unicode_string:
    :return: string.
    """

    if unicode_string is not None:
        return str(unicode_string)
    return None


def get_average_summary_dictionary_prmon(path):
    """
    Loop over the memory monitor output file and create the averaged summary dictionary.

    prmon keys:
    'Time', 'nprocs', 'nthreads', 'pss', 'rchar', 'read_bytes', 'rss', 'rx_bytes',
    'rx_packets', 'stime', 'swap', 'tx_bytes', 'tx_packets', 'utime', 'vmem', 'wchar',
    'write_bytes', 'wtime'

    The function uses the first line in the output file to define the dictionary keys used
    later in the function. This means that any change in the format such as new columns
    will be handled automatically.

    :param path: path to memory monitor txt output file (string).
    :return: summary dictionary.
    """

    summary_dictionary = {}

    # get the raw memory monitor output, convert to dictionary
    dictionary = convert_text_file_to_dictionary(path)

    if dictionary:
        # Calculate averages and store all values
        summary_dictionary = {"Max": {}, "Avg": {}, "Other": {}}

        def filter_value(value):
            """ Inline function used to remove any string or None values from data. """
            if type(value) == str or value is None:
                return False
            else:
                return True

        keys = ['vmem', 'pss', 'rss', 'swap']
        values = {}
        for key in keys:
            value_list = list(filter(filter_value, dictionary.get(key, 0)))  # Python 2/3
            n = len(value_list)
            average = int(float(sum(value_list)) / float(n)) if n > 0 else 0
            maximum = max(value_list)
            values[key] = {'avg': average, 'max': maximum}

        summary_dictionary["Max"] = {"maxVMEM": values['vmem'].get('max'), "maxPSS": values['pss'].get('max'),
                                     "maxRSS": values['rss'].get('max'), "maxSwap": values['swap'].get('max')}
        summary_dictionary["Avg"] = {"avgVMEM": values['vmem'].get('avg'), "avgPSS": values['pss'].get('avg'),
                                     "avgRSS": values['rss'].get('avg'), "avgSwap": values['swap'].get('avg')}

        # add the last of the rchar, .., values
        keys = ['rchar', 'wchar', 'read_bytes', 'write_bytes', 'nprocs']
        # warning: should read_bytes/write_bytes be reported as rbytes/wbytes?
        for key in keys:
            value = get_last_value(dictionary.get(key, None))
            if value:
                summary_dictionary["Other"][key] = value

    return summary_dictionary


def get_metadata_dict_from_txt(path, storejson=False, jobid=None):
    """
    Convert memory monitor text output to json, store it, and return a selection as a dictionary.

    :param path:
    :param storejson: store dictionary on disk if True (boolean).
    :param jobid: job id (string).
    :return: prmon metadata (dictionary).
    """

    # get the raw memory monitor output, convert to dictionary
    dictionary = convert_text_file_to_dictionary(path)

    if dictionary and storejson:
        # add metadata
        dictionary['type'] = 'MemoryMonitorData'
        dictionary['pandaid'] = jobid

        path = os.path.join(os.path.dirname(path), get_memory_monitor_output_filename(suffix='json'))
        logger.debug('writing prmon dictionary to: %s' % path)
        write_json(path, dictionary)
    else:
        logger.debug('nothing to write (no prmon dictionary)')

    # filter dictionary?
    # ..

    return dictionary


def convert_text_file_to_dictionary(path):
    """
    Convert row-column text file to dictionary.
    User first row identifiers as dictionary keys.
    Note: file must follow the convention:
        NAME1   NAME2   ..
        value1  value2  ..
        ..      ..      ..

    :param path: path to file (string).
    :return: dictionary.
    """

    summary_keys = []  # to keep track of content
    header_locked = False
    dictionary = {}

    with open(path) as f:
        for line in f:
            line = convert_unicode_string(line)
            if line != "":
                try:
                    # Remove empty entries from list (caused by multiple \t)
                    _l = line.replace('\n', '')
                    if is_python3():
                        _l = [_f for _f in _l.split('\t') if _f]  # Python 3
                    else:
                        _l = filter(None, _l.split('\t'))  # Python 2

                    # define dictionary keys
                    if type(_l[0]) == str and not header_locked:
                        summary_keys = _l
                        for key in _l:
                            dictionary[key] = []
                        header_locked = True
                    else:  # sort the memory measurements in the correct columns
                        for i, key in enumerate(_l):
                            # for key in _l:
                            key_entry = summary_keys[i]  # e.g. Time
                            value = convert_to_int(key)
                            dictionary[key_entry].append(value)
                except Exception:
                    logger.warning("unexpected format of utility output: %s" % line)

    return dictionary


def get_last_value(value_list):
    value = None
    if value_list:
        value = value_list[-1]
    return value


def get_average_summary_dictionary(path):
    """
    Loop over the memory monitor output file and create the averaged summary dictionary.

    :param path: path to memory monitor txt output file (string).
    :return: summary dictionary.
    """

    maxvmem = -1
    maxrss = -1
    maxpss = -1
    maxswap = -1
    avgvmem = 0
    avgrss = 0
    avgpss = 0
    avgswap = 0
    totalvmem = 0
    totalrss = 0
    totalpss = 0
    totalswap = 0
    n = 0
    summary_dictionary = {}

    rchar = None
    wchar = None
    rbytes = None
    wbytes = None

    first = True
    with open(path) as f:
        for line in f:
            # Skip the first line
            if first:
                first = False
                continue
            line = convert_unicode_string(line)
            if line != "":
                try:
                    # Remove empty entries from list (caused by multiple \t)
                    if is_python3():
                        _l = [_f for _f in line.split('\t') if _f]  # Python 3
                    else:
                        _l = filter(None, line.split('\t'))  # Python 2
                    # _time = _l[0]  # 'Time' not user
                    vmem = _l[1]
                    pss = _l[2]
                    rss = _l[3]
                    swap = _l[4]
                    # note: the last rchar etc values will be reported
                    if len(_l) == 9:
                        rchar = int(_l[5])
                        wchar = int(_l[6])
                        rbytes = int(_l[7])
                        wbytes = int(_l[8])
                    else:
                        rchar = None
                        wchar = None
                        rbytes = None
                        wbytes = None
                except Exception:
                    logger.warning("unexpected format of utility output: %s (expected format: Time, VMEM,"
                                   " PSS, RSS, Swap [, RCHAR, WCHAR, RBYTES, WBYTES])" % (line))
                else:
                    # Convert to int
                    ec1, maxvmem, totalvmem = get_max_memory_monitor_value(vmem, maxvmem, totalvmem)
                    ec2, maxpss, totalpss = get_max_memory_monitor_value(pss, maxpss, totalpss)
                    ec3, maxrss, totalrss = get_max_memory_monitor_value(rss, maxrss, totalrss)
                    ec4, maxswap, totalswap = get_max_memory_monitor_value(swap, maxswap, totalswap)
                    if ec1 or ec2 or ec3 or ec4:
                        logger.warning("will skip this row of numbers due to value exception: %s" % (line))
                    else:
                        n += 1

        # Calculate averages and store all values
        summary_dictionary = {"Max": {}, "Avg": {}, "Other": {}}
        summary_dictionary["Max"] = {"maxVMEM": maxvmem, "maxPSS": maxpss, "maxRSS": maxrss, "maxSwap": maxswap}
        if rchar:
            summary_dictionary["Other"]["rchar"] = rchar
        if wchar:
            summary_dictionary["Other"]["wchar"] = wchar
        if rbytes:
            summary_dictionary["Other"]["rbytes"] = rbytes
        if wbytes:
            summary_dictionary["Other"]["wbytes"] = wbytes
        if n > 0:
            avgvmem = int(float(totalvmem) / float(n))
            avgpss = int(float(totalpss) / float(n))
            avgrss = int(float(totalrss) / float(n))
            avgswap = int(float(totalswap) / float(n))
        summary_dictionary["Avg"] = {"avgVMEM": avgvmem, "avgPSS": avgpss, "avgRSS": avgrss, "avgSwap": avgswap}

    return summary_dictionary


def get_memory_values(workdir, name=""):
    """
    Find the values in the memory monitor output file.

    In case the summary JSON file has not yet been produced, create a summary dictionary with the same format
    using the output text file (produced by the memory monitor and which is updated once per minute).

    FORMAT:
       {"Max":{"maxVMEM":40058624,"maxPSS":10340177,"maxRSS":16342012,"maxSwap":16235568},
        "Avg":{"avgVMEM":19384236,"avgPSS":5023500,"avgRSS":6501489,"avgSwap":5964997},
        "Other":{"rchar":NN,"wchar":NN,"rbytes":NN,"wbytes":NN}}

    :param workdir: relevant work directory (string).
    :param name: name of memory monitor (string).
    :return: memory values dictionary.
    """

    summary_dictionary = {}

    # Get the path to the proper memory info file (priority ordered)
    path = get_memory_monitor_info_path(workdir, allowtxtfile=True)
    if os.path.exists(path):
        logger.info("using path: %s (trf name=%s)" % (path, name))

        # Does a JSON summary file exist? If so, there's no need to calculate maximums and averages in the pilot
        if path.lower().endswith('json'):
            # Read the dictionary from the JSON file
            summary_dictionary = read_json(path)
        else:
            # Loop over the output file, line by line, and look for the maximum PSS value
            if name == "prmon":
                summary_dictionary = get_average_summary_dictionary_prmon(path)
            else:
                summary_dictionary = get_average_summary_dictionary(path)
            logger.debug('summary_dictionary=%s (trf name=%s)' % (str(summary_dictionary), name))
    else:
        if path == "":
            logger.warning("filename not set for memory monitor output")
        else:
            # Normally this means that the memory output file has not been produced yet
            pass

    return summary_dictionary


def post_memory_monitor_action(job):
    """
    Perform post action items for memory monitor.

    :param job: job object.
    :return:
    """

    nap = 3
    path1 = os.path.join(job.workdir, get_memory_monitor_summary_filename())
    path2 = os.environ.get('PILOT_HOME')
    i = 0
    maxretry = 20
    while i <= maxretry:
        if os.path.exists(path1):
            break
        logger.info("taking a short nap (%d s) to allow the memory monitor to finish writing to the summary file (#%d/#%d)"
                    % (nap, i, maxretry))
        time.sleep(nap)
        i += 1

    try:
        copy(path1, path2)
    except Exception as e:
        logger.warning('failed to copy memory monitor output: %s' % e)


def precleanup():
    """
    Pre-cleanup at the beginning of the job to remove any pre-existing files from previous jobs in the main work dir.

    :return:
    """

    logger.debug('performing pre-cleanup of potentially pre-existing files from earlier job in main work dir')
    path = os.path.join(os.environ.get('PILOT_HOME'), get_memory_monitor_summary_filename())
    if os.path.exists(path):
        logger.info('removing no longer needed file: %s' % path)
        remove(path)
