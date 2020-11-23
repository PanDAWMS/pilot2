#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2020

import json
import os
import re
from glob import glob

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException, BadXML
from pilot.util.config import config
from pilot.util.filehandling import get_guid, tail, grep, open_file, read_file, scan_file  #, write_file
from pilot.util.math import convert_mb_to_b
from pilot.util.workernode import get_local_disk_space

from .common import update_job_data, parse_jobreport_data
from .metadata import get_metadata_from_xml, get_total_number_of_events, get_guid_from_xml

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def interpret(job):
    """
    Interpret the payload, look for specific errors in the stdout.

    :param job: job object
    :return: exit code (payload) (int).
    """

    exit_code = 0

    # extract errors from job report
    process_job_report(job)
    if job.piloterrorcodes:
        logger.warning('aborting payload error diagnosis since an error has already been set')
        return -1

    if job.exitcode != 0:
        exit_code = job.exitcode

    # check for special errors
    if exit_code == 146:
        logger.warning('user tarball was not downloaded (payload exit code %d)' % exit_code)
        set_error_nousertarball(job)
    elif exit_code == 160:
        logger.info('ignoring harmless preprocess exit code %d' % exit_code)
        job.transexitcode = 0
        job.exitcode = 0
        exit_code = 0

    # extract special information, e.g. number of events
    try:
        extract_special_information(job)
    except PilotException as error:
        logger.error('PilotException caught while extracting special job information: %s' % error)
        exit_code = error.get_error_code()
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code)

    # interpret the exit info from the payload
    interpret_payload_exit_info(job)

    return exit_code


def interpret_payload_exit_info(job):
    """
    Interpret the exit info from the payload

    :param job: job object.
    :return:
    """

    # try to identify out of memory errors in the stderr
    if is_out_of_memory(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADOUTOFMEMORY, priority=True)
        return

    # look for specific errors in the stdout (tail)
    if is_installation_error(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.MISSINGINSTALLATION, priority=True)
        return

    # did AtlasSetup fail?
    if is_atlassetup_error(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.ATLASSETUPFATAL, priority=True)
        return

    # did the payload run out of space?
    if is_out_of_space(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOLOCALSPACE, priority=True)

        # double check local space
        spaceleft = convert_mb_to_b(get_local_disk_space(os.getcwd()))  # B (diskspace is in MB)
        logger.info('verifying local space: %d B' % spaceleft)
        return

    # look for specific errors in the stdout (full)
    if is_nfssqlite_locking_problem(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NFSSQLITE, priority=True)
        return

    # is the user tarball missing on the server?
    if is_user_code_missing(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.MISSINGUSERCODE, priority=True)
        return

    # set a general Pilot error code if the payload error could not be identified
    if job.transexitcode == 0 and job.exitcode != 0:
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.UNKNOWNPAYLOADFAILURE, priority=True)


def is_out_of_memory(job):
    """
    Did the payload run out of memory?

    :param job: job object.
    :return: Boolean. (note: True means the error was found)
    """

    out_of_memory = False

    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)

    files = {stderr: ["FATAL out of memory: taking the application down"], stdout: ["St9bad_alloc", "std::bad_alloc"]}
    for path in files:
        if os.path.exists(path):
            logger.info('looking for out-of-memory errors in %s' % os.path.basename(path))
            if os.path.getsize(path) > 0:
                matched_lines = grep(files[path], path)
                if len(matched_lines) > 0:
                    logger.warning("identified an out of memory error in %s %s:" % (job.payload, os.path.basename(path)))
                    for line in matched_lines:
                        logger.info(line)
                    out_of_memory = True
        else:
            logger.warning('file does not exist: %s (cannot look for out-of-memory error in it)')

    return out_of_memory


def is_user_code_missing(job):
    """
    Is the user code (tarball) missing on the server?

    :param job: job object.
    :return: Boolean. (note: True means the error was found)
    """

    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    error_messages = ["ERROR: unable to fetch source tarball from web"]

    return scan_file(stdout,
                     error_messages,
                     warning_message="identified an \'%s\' message in %s" % (error_messages[0], os.path.basename(stdout)))


def is_out_of_space(job):
    """
    Did the disk run out of space?

    :param job: job object.
    :return: Boolean. (note: True means the error was found)
    """

    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
    error_messages = ["No space left on device"]

    return scan_file(stderr,
                     error_messages,
                     warning_message="identified a \'%s\' message in %s" % (error_messages[0], os.path.basename(stderr)))


def is_installation_error(job):
    """
    Did the payload fail to run? (Due to faulty/missing installation).

    :param job: job object.
    :return: Boolean. (note: True means the error was found)
    """

    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    _tail = tail(stdout)
    res_tmp = _tail[:1024]
    if res_tmp[0:3] == "sh:" and 'setup.sh' in res_tmp and 'No such file or directory' in res_tmp:
        return True
    else:
        return False


def is_atlassetup_error(job):
    """
    Did AtlasSetup fail with a fatal error?

    :param job: job object.
    :return: Boolean. (note: True means the error was found)
    """

    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    _tail = tail(stdout)
    res_tmp = _tail[:2048]
    if "AtlasSetup(FATAL): Fatal exception" in res_tmp:
        logger.warning('AtlasSetup FATAL failure detected')
        return True
    else:
        return False


def is_nfssqlite_locking_problem(job):
    """
    Were there any NFS SQLite locking problems?

    :param job: job object.
    :return: Boolean. (note: True means the error was found)
    """

    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    error_messages = ["prepare 5 database is locked", "Error SQLiteStatement"]

    return scan_file(stdout,
                     error_messages,
                     warning_message="identified an NFS/Sqlite locking problem in %s" % os.path.basename(stdout))


def extract_special_information(job):
    """
    Extract special information from different sources, such as number of events and data base fields.

    :param job: job object.
    :return:
    """

    # try to find the number(s) of processed events (will be set in the relevant job fields)
    find_number_of_events(job)

    # get the DB info from the jobReport
    try:
        find_db_info(job)
    except Exception as e:
        logger.warning('detected problem with parsing job report (in find_db_info()): %s' % e)


def find_number_of_events(job):
    """
    Locate the number of events.

    :param job: job object.
    :return:
    """

    if job.nevents:
        logger.info('number of events already known: %d' % job.nevents)
        return

    logger.info('looking for number of processed events (source #1: jobReport.json)')
    find_number_of_events_in_jobreport(job)
    if job.nevents > 0:
        logger.info('found %d processed events' % job.nevents)
        return

    logger.info('looking for number of processed events (source #2: metadata.xml)')
    find_number_of_events_in_xml(job)
    if job.nevents > 0:
        logger.info('found %d processed events' % job.nevents)
        return

    logger.info('looking for number of processed events (source #3: athena summary file(s)')
    n1, n2 = process_athena_summary(job)
    if n1 > 0:
        job.nevents = n1
        logger.info('found %d processed (read) events' % job.nevents)
    if n2 > 0:
        job.neventsw = n2
        logger.info('found %d processed (written) events' % job.neventsw)


def find_number_of_events_in_jobreport(job):
    """
    Try to find the number of events in the jobReport.json file.

    :param job: job object.
    :return:
    """

    try:
        work_attributes = parse_jobreport_data(job.metadata)
    except Exception as e:
        logger.warning('exception caught while parsing job report: %s' % e)
        return

    if 'nEvents' in work_attributes:
        try:
            n_events = work_attributes.get('nEvents')
            if n_events:
                job.nevents = int(n_events)
        except ValueError as e:
            logger.warning('failed to convert number of events to int: %s' % e)


def find_number_of_events_in_xml(job):
    """
    Try to find the number of events in the metadata.xml file.

    :param job: job object.
    :raises: BadXML exception if metadata cannot be parsed.
    :return:
    """

    try:
        metadata = get_metadata_from_xml(job.workdir)
    except Exception as e:
        msg = "Exception caught while interpreting XML: %s" % e
        raise BadXML(msg)

    if metadata:
        nevents = get_total_number_of_events(metadata)
        if nevents > 0:
            job.nevents = nevents


def process_athena_summary(job):
    """
    Try to find the number of events in the Athena summary file.

    :param job: job object.
    :return: number of read events (int), number of written events (int).
    """

    n1 = 0
    n2 = 0
    file_pattern_list = ['AthSummary*', 'AthenaSummary*']

    file_list = []
    # loop over all patterns in the list to find all possible summary files
    for file_pattern in file_pattern_list:
        # get all the summary files for the current file pattern
        files = glob(os.path.join(job.workdir, file_pattern))
        # append all found files to the file list
        for summary_file in files:
            file_list.append(summary_file)

    if file_list == [] or file_list == ['']:
        logger.info("did not find any athena summary files")
    else:
        # find the most recent and the oldest files
        recent_summary_file, recent_time, oldest_summary_file, oldest_time = \
            find_most_recent_and_oldest_summary_files(file_list)
        if oldest_summary_file == recent_summary_file:
            logger.info("summary file %s will be processed for errors and number of events" %
                        os.path.basename(oldest_summary_file))
        else:
            logger.info("most recent summary file %s (updated at %d) will be processed for errors [to be implemented]" %
                        (os.path.basename(recent_summary_file), recent_time))
            logger.info("oldest summary file %s (updated at %d) will be processed for number of events" %
                        (os.path.basename(oldest_summary_file), oldest_time))

        # Get the number of events from the oldest summary file
        n1, n2 = get_number_of_events_from_summary_file(oldest_summary_file)
        logger.info("number of events: %d (read)" % n1)
        logger.info("number of events: %d (written)" % n2)

    return n1, n2


def find_most_recent_and_oldest_summary_files(file_list):
    """
    Find the most recent and the oldest athena summary files.
    :param file_list: list of athena summary files (list of strings).
    :return: most recent summary file (string), recent time (int), oldest summary file (string), oldest time (int).
    """

    oldest_summary_file = ""
    recent_summary_file = ""
    oldest_time = 9999999999
    recent_time = 0
    if len(file_list) > 1:
        for summary_file in file_list:
            # get the modification time
            try:
                st_mtime = os.path.getmtime(summary_file)
            except Exception as e:  # Python 2/3
                logger.warning("could not read modification time of file %s: %s" % (summary_file, e))
            else:
                if st_mtime > recent_time:
                    recent_time = st_mtime
                    recent_summary_file = summary_file
                if st_mtime < oldest_time:
                    oldest_time = st_mtime
                    oldest_summary_file = summary_file
    else:
        oldest_summary_file = file_list[0]
        recent_summary_file = oldest_summary_file
        oldest_time = os.path.getmtime(oldest_summary_file)
        recent_time = oldest_time

    return recent_summary_file, recent_time, oldest_summary_file, oldest_time


def get_number_of_events_from_summary_file(oldest_summary_file):
    """
    Get the number of events from the oldest summary file.

    :param oldest_summary_file: athena summary file (filename, str).
    :return: number of read events (int), number of written events (int).
    """

    n1 = 0
    n2 = 0

    f = open_file(oldest_summary_file, 'r')
    if f:
        lines = f.readlines()
        f.close()

        if len(lines) > 0:
            for line in lines:
                if "Events Read:" in line:
                    try:
                        n1 = int(re.match(r'Events Read\: *(\d+)', line).group(1))  # Python 3 (added r)
                    except ValueError as e:
                        logger.warning('failed to convert number of read events to int: %s' % e)
                if "Events Written:" in line:
                    try:
                        n2 = int(re.match(r'Events Written\: *(\d+)', line).group(1))  # Python 3 (added r)
                    except ValueError as e:
                        logger.warning('failed to convert number of written events to int: %s' % e)
                if n1 > 0 and n2 > 0:
                    break
        else:
            logger.warning('failed to get number of events from empty summary file')

    # Get the errors from the most recent summary file
    # ...

    return n1, n2


def find_db_info(job):
    """
    Find the DB info in the jobReport

    :param job: job object.
    :return:
    """

    work_attributes = parse_jobreport_data(job.metadata)
    if '__db_time' in work_attributes:
        try:
            job.dbtime = int(work_attributes.get('__db_time'))
        except ValueError as e:
            logger.warning('failed to convert dbtime to int: %s' % e)
        logger.info('dbtime (total): %d' % job.dbtime)
    if '__db_data' in work_attributes:
        try:
            job.dbdata = work_attributes.get('__db_data')
        except ValueError as e:
            logger.warning('failed to convert dbdata to int: %s' % e)
        logger.info('dbdata (total): %d' % job.dbdata)


def set_error_nousertarball(job):
    """
    Set error code for NOUSERTARBALL.

    :param job: job object.
    :return:
    """

    # get the tail of the stdout since it will contain the URL of the user log
    filename = os.path.join(job.workdir, config.Payload.payloadstdout)
    _tail = tail(filename)
    _tail += 'http://someurl.se/path'
    if _tail:
        # try to extract the tarball url from the tail
        tarball_url = extract_tarball_url(_tail)

        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOUSERTARBALL)
        job.piloterrorcode = errors.NOUSERTARBALL
        job.piloterrordiag = "User tarball %s cannot be downloaded from PanDA server" % tarball_url


def extract_tarball_url(_tail):
    """
    Extract the tarball URL for missing user code if possible from stdout tail.

    :param _tail: tail of payload stdout (string).
    :return: url (string).
    """

    tarball_url = "(source unknown)"

    if "https://" in _tail or "http://" in _tail:
        pattern = r"(https?\:\/\/.+)"
        found = re.findall(pattern, _tail)
        if len(found) > 0:
            tarball_url = found[0]

    return tarball_url


def process_metadata_from_xml(job):
    """
    Extract necessary metadata from XML when job report is not available.

    :param job: job object.
    :return: [updated job object - return not needed].
    """

    # get the metadata from the xml file instead, which must exist for most production transforms
    path = os.path.join(job.workdir, config.Payload.metadata)
    if os.path.exists(path):
        job.metadata = read_file(path)
    else:
        if not job.is_analysis() and job.transformation != 'Archive_tf.py':
            diagnostics = 'metadata does not exist: %s' % path
            logger.warning(diagnostics)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOPAYLOADMETADATA)
            job.piloterrorcode = errors.NOPAYLOADMETADATA
            job.piloterrordiag = diagnostics

    # add missing guids
    for dat in job.outdata:
        if not dat.guid:
            # try to read it from the metadata before the last resort of generating it
            metadata = None
            try:
                metadata = get_metadata_from_xml(job.workdir)
            except Exception as e:
                msg = "Exception caught while interpreting XML: %s (ignoring it, but guids must now be generated)" % e
                logger.warning(msg)
            if metadata:
                dat.guid = get_guid_from_xml(metadata, dat.lfn)
                logger.info('read guid for lfn=%s from xml: %s' % (dat.lfn, dat.guid))
            else:
                dat.guid = get_guid()
                logger.info('generated guid for lfn=%s: %s' % (dat.lfn, dat.guid))


def process_job_report(job):
    """
    Process the job report produced by the payload/transform if it exists.
    Payload error codes and diagnostics, as well as payload metadata (for output files) and stageout type will be
    extracted. The stageout type is either "all" (i.e. stage-out both output and log files) or "log" (i.e. only log file
    will be staged out).
    Note: some fields might be experiment specific. A call to a user function is therefore also done.

    :param job: job dictionary will be updated by the function and several fields set.
    :return:
    """

    # get the job report
    path = os.path.join(job.workdir, config.Payload.jobreport)
    if not os.path.exists(path):
        logger.warning('job report does not exist: %s' % path)

        # get the metadata from the xml file instead, which must exist for most production transforms
        process_metadata_from_xml(job)
    else:
        with open(path) as data_file:
            # compulsory field; the payload must produce a job report (see config file for file name), attach it to the
            # job object
            job.metadata = json.load(data_file)

            #
            update_job_data(job)

            # compulsory fields
            try:
                job.exitcode = job.metadata['exitCode']
            except Exception as e:
                logger.warning('could not find compulsory payload exitCode in job report: %s (will be set to 0)' % e)
                job.exitcode = 0
            else:
                logger.info('extracted exit code from job report: %d' % job.exitcode)
            try:
                job.exitmsg = job.metadata['exitMsg']
            except Exception as e:
                logger.warning('could not find compulsory payload exitMsg in job report: %s '
                               '(will be set to empty string)' % e)
                job.exitmsg = ""
            else:
                # assign special payload error code
                if "got a SIGSEGV signal" in job.exitmsg:
                    diagnostics = 'Invalid memory reference or a segmentation fault in payload: %s (job report)' % \
                                  job.exitmsg
                    logger.warning(diagnostics)
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADSIGSEGV)
                    job.piloterrorcode = errors.PAYLOADSIGSEGV
                    job.piloterrordiag = diagnostics
                else:
                    logger.info('extracted exit message from job report: %s' % job.exitmsg)
                    if job.exitmsg != 'OK':
                        job.exeerrordiag = job.exitmsg
                        job.exeerrorcode = job.exitcode

            if job.exitcode != 0:
                # get list with identified errors in job report
                job_report_errors = get_job_report_errors(job.metadata)

                # is it a bad_alloc failure?
                bad_alloc, diagnostics = is_bad_alloc(job_report_errors)
                if bad_alloc:
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.BADALLOC)
                    job.piloterrorcode = errors.BADALLOC
                    job.piloterrordiag = diagnostics


def get_job_report_errors(job_report_dictionary):
    """
    Extract the error list from the jobReport.json dictionary.
    The returned list is scanned for special errors.

    :param job_report_dictionary:
    :return: job_report_errors list.
    """

    job_report_errors = []
    if 'reportVersion' in job_report_dictionary:
        logger.info("scanning jobReport (v %s) for error info" % job_report_dictionary.get('reportVersion'))
    else:
        logger.warning("jobReport does not have the reportVersion key")

    if 'executor' in job_report_dictionary:
        try:
            error_details = job_report_dictionary['executor'][0]['logfileReport']['details']['ERROR']
        except Exception as e:
            logger.warning("WARNING: aborting jobReport scan: %s" % e)
        else:
            try:
                for m in error_details:
                    job_report_errors.append(m['message'])
            except Exception as e:
                logger.warning("did not get a list object: %s" % e)
    else:
        logger.warning("jobReport does not have the executor key (aborting)")

    return job_report_errors


def is_bad_alloc(job_report_errors):
    """
    Check for bad_alloc errors.

    :param job_report_errors: list with errors extracted from the job report.
    :return: bad_alloc (bool), diagnostics (string).
    """

    bad_alloc = False
    diagnostics = ""
    for m in job_report_errors:
        if "bad_alloc" in m:
            logger.warning("encountered a bad_alloc error: %s" % m)
            bad_alloc = True
            diagnostics = m
            break

    return bad_alloc, diagnostics


def get_log_extracts(job, state):
    """
    Extract special warnings and other other info from special logs.
    This function also discovers if the payload had any outbound connections.

    :param job: job object.
    :param state: job state (string).
    :return: log extracts (string).
    """

    logger.info("building log extracts (sent to the server as \'pilotLog\')")

    # did the job have any outbound connections?
    # look for the pandatracerlog.txt file, produced if the user payload attempted any outgoing connections
    extracts = get_panda_tracer_log(job)

    # for failed/holding jobs, add extracts from the pilot log file, but always add it to the pilot log itself
    _extracts = get_pilot_log_extracts(job)
    if _extracts != "":
        logger.warning('detected the following tail of warning/fatal messages in the pilot log:\n%s' % _extracts)
        if state == 'failed' or state == 'holding':
            extracts += _extracts

    # add extracts from payload logs
    # (see buildLogExtracts in Pilot 1)

    return extracts


def get_panda_tracer_log(job):
    """
    Return the contents of the PanDA tracer log if it exists.
    This file will contain information about outbound connections.

    :param job: job object.
    :return: log extracts from pandatracerlog.txt (string).
    """

    extracts = ""

    tracerlog = os.path.join(job.workdir, "pandatracerlog.txt")
    if os.path.exists(tracerlog):
        # only add if file is not empty
        if os.path.getsize(tracerlog) > 0:
            message = "PandaID=%s had outbound connections: " % (job.jobid)
            extracts += message
            message = read_file(tracerlog)
            extracts += message
            logger.warning(message)
        else:
            logger.info("PanDA tracer log (%s) has zero size (no outbound connections detected)" % tracerlog)
    else:
        logger.debug("PanDA tracer log does not exist: %s (ignoring)" % tracerlog)

    return extracts


def get_pilot_log_extracts(job):
    """
    Get the extracts from the pilot log (warning/fatal messages, as well as tail of the log itself).

    :param job: job object.
    :return: tail of pilot log (string).
    """

    extracts = ""

    path = os.path.join(job.workdir, config.Pilot.pilotlog)
    if os.path.exists(path):
        # get the last 20 lines of the pilot log in case it contains relevant error information
        _tail = tail(path, nlines=20)
        if _tail != "":
            if extracts != "":
                extracts += "\n"
            extracts += "- Log from %s -\n" % config.Pilot.pilotlog
            extracts += _tail

        # grep for fatal/critical errors in the pilot log
        #errormsgs = ["FATAL", "CRITICAL", "ERROR"]
        #matched_lines = grep(errormsgs, path)
        #_extracts = ""
        #if len(matched_lines) > 0:
        #    logger.debug("dumping warning messages from %s:\n" % os.path.basename(path))
        #    for line in matched_lines:
        #        _extracts += line + "\n"
        #if _extracts != "":
        #    if config.Pilot.error_log != "":
        #        path = os.path.join(job.workdir, config.Pilot.error_log)
        #        write_file(path, _extracts)
        #    extracts += "\n- Error messages from %s -\n" % config.Pilot.pilotlog
        #    extracts += _extracts
    else:
        logger.warning('pilot log file does not exist: %s' % path)

    return extracts
