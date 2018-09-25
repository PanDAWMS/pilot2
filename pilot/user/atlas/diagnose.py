#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

import json
import os
import re
from glob import glob

from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import get_logger
from pilot.util.config import config
from pilot.util.filehandling import get_guid, tail, open_file

from .common import update_job_data, parse_jobreport_data
from .metadata import get_metadata_from_xml, get_total_number_of_events

errors = ErrorCodes()


def interpret(job):
    """
    Interpret the payload, look for specific errors in the stdout.

    :param job: job object
    :return: exit code (payload) (int).
    """

    exit_code = 0
    log = get_logger(job.jobid)

    # extract errors from job report
    process_job_report(job)

    if job.exitcode == 0:
        pass
    else:
        exit_code = job.exitcode

    # check for special errors
    if exit_code == 146:
        log.warning('user tarball was not downloaded (payload exit code %d)' % exit_code)
        set_error_nousertarball(job)

    # extract special information, e.g. number of events
    extract_special_information(job)

    # interpret the stdout from the payload

    log.debug('payload interpret function ended with exit_code: %d' % exit_code)

    return exit_code


def extract_special_information(job):
    """
    Extract special information from different sources, such as number of events and data base fields.

    :param job: job object.
    :return:
    """

    # try to find the number(s) of processed events (will be set in the relevant job fields)
    find_number_of_events(job)

    # get the DB info from the jobReport
    find_db_info(job)


def find_number_of_events(job):
    """
    Locate the number of events.

    :param job: job object.
    :return:
    """

    log = get_logger(job.jobid)

    log.info('looking for number of processed events (source #1: jobReport.json)')
    find_number_of_events_in_jobreport(job)
    if job.nevents > 0:
        log.info('found %d processed events' % job.nevents)
        return

    log.info('looking for number of processed events (source #2: metadata.xml)')
    find_number_of_events_in_xml(job)
    if job.nevents > 0:
        log.info('found %d processed events' % job.nevents)
        return

    log.info('looking for number of processed events (source #3: athena summary file(s)')
    n1, n2 = process_athena_summary(job)
    if n1 > 0:
        job.nevents = n1
        log.info('found %d processed (read) events' % job.nevents)
    if n2 > 0:
        job.neventsw = n2
        log.info('found %d processed (written) events' % job.neventsw)


def find_number_of_events_in_jobreport(job):
    """
    Try to find the number of events in the jobReport.json file.

    :param job: job object.
    :return:
    """

    work_attributes = parse_jobreport_data(job.metadata)
    if 'n_events' in work_attributes:
        job.nevents = work_attributes.get('n_events')


def find_number_of_events_in_xml(job):
    """
    Try to find the number of events in the metadata.xml file.

    :param job: job object.
    :return:
    """

    metadata = get_metadata_from_xml(job.workdir)
    nevents = get_total_number_of_events(metadata)
    if nevents > 0:
        job.nevents = nevents


def process_athena_summary(job):
    """
    Try to find the number of events in the Athena summary file.

    :param job: job object.
    :return: number of read events (int), number of written events (int).
    """

    log = get_logger(job.jobid)

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
        log.info("did not find any athena summary files")
    else:
        # find the most recent and the oldest files
        oldest_summary_file = ""
        recent_summary_file = ""
        oldest_time = 9999999999
        recent_time = 0
        if len(file_list) > 1:
            for summary_file in file_list:
                # get the modification time
                try:
                    st_mtime = os.path.getmtime(summary_file)
                except Exception, e:
                    log.warning("could not read modification time of file %s: %s" % (summary_file, e))
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

        if oldest_summary_file == recent_summary_file:
            log.info("summary file %s will be processed for errors and number of events" %
                     os.path.basename(oldest_summary_file))
        else:
            log.info("most recent summary file %s (updated at %d) will be processed for errors" %
                     (os.path.basename(recent_summary_file), recent_time))
            log.info("oldest summary file %s (updated at %d) will be processed for number of events" %
                     (os.path.basename(oldest_summary_file), oldest_time))

        # Get the number of events from the oldest summary file
        f = open_file(oldest_summary_file, 'r')
        if f:
            lines = f.readlines()
            f.close()

            if len(lines) > 0:
                for line in lines:
                    if "Events Read:" in line:
                        n1 = int(re.match('Events Read\: *(\d+)', line).group(1))
                    if "Events Written:" in line:
                        n2 = int(re.match('Events Written\: *(\d+)', line).group(1))
                    if n1 > 0 and n2 > 0:
                        break
            else:
                log.warning('failed to get number of events from empty summary file')

            log.info("number of events: %d (read)" % n1)
            log.info("number of events: %d (written)" % n2)

        # Get the errors from the most recent summary file
        # ...

    return n1, n2


def find_db_info(job):
    """
    Find the DB info in the jobReport

    :param job: job object.
    :return:
    """

    log = get_logger(job.jobid)

    work_attributes = parse_jobreport_data(job.metadata)
    if '__db_time' in work_attributes:
        job.dbtime = work_attributes.get('__db_time')
        log.info('dbtime (total): %d' % job.dbtime)
    if '__db_data' in work_attributes:
        job.dbdata = work_attributes.get('__db_data')
        log.info('dbdata (total): %d' % job.dbdata)


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

    log = get_logger(job.jobid)

    # get the job report
    path = os.path.join(job.workdir, config.Payload.jobreport)
    if not os.path.exists(path):
        log.warning('job report does not exist: %s (any missing output file guids must be generated)' % path)

        # add missing guids
        for dat in job.outdata:
            if not dat.guid:
                dat.guid = get_guid()
                log.warning('guid not set: generated guid=%s for lfn=%s' % (dat.guid, dat.lfn))

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
                log.warning('could not find compulsory payload exitCode in job report: %s (will be set to 0)' % e)
                job.exitcode = 0
            else:
                log.info('extracted exit code from job report: %d' % job.exitcode)
            try:
                job.exitmsg = job.metadata['exitMsg']
            except Exception as e:
                log.warning('could not find compulsory payload exitMsg in job report: %s '
                            '(will be set to empty string)' % e)
                job.exitmsg = ""
            else:
                log.info('extracted exit message from job report: %s' % job.exitmsg)

            if job.exitcode != 0:
                # get list with identified errors in job report
                job_report_errors = get_job_report_errors(job.metadata, log)

                # is it a bad_alloc failure?
                bad_alloc, diagnostics = is_bad_alloc(job_report_errors, log)
                if bad_alloc:
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.BADALLOC)
                    job.piloterrorcode = errors.BADALLOC
                    job.piloterrordiag = diagnostics


def get_job_report_errors(job_report_dictionary, log):
    """
    Extract the error list from the jobReport.json dictionary.
    The returned list is scanned for special errors.

    :param job_report_dictionary:
    :param log: job logger object.
    :return: job_report_errors list.
    """

    job_report_errors = []
    if 'reportVersion' in job_report_dictionary:
        log.info("scanning jobReport (v %s) for error info" % job_report_dictionary.get('reportVersion'))
    else:
        log.warning("jobReport does not have the reportVersion key")

    if 'executor' in job_report_dictionary:
        try:
            error_details = job_report_dictionary['executor'][0]['logfileReport']['details']['ERROR']
        except Exception as e:
            log.warning("WARNING: aborting jobReport scan: %s" % e)
        else:
            try:
                for m in error_details:
                    job_report_errors.append(m['message'])
            except Exception as e:
                log.warning("did not get a list object: %s" % e)
    else:
        log.warning("jobReport does not have the executor key (aborting)")

    return job_report_errors


def is_bad_alloc(job_report_errors, log):
    """
    Check for bad_alloc errors.

    :param job_report_errors: list with errors extracted from the job report.
    :param log: job logger object.
    :return: bad_alloc (bool), diagnostics (string).
    """

    bad_alloc = False
    diagnostics = ""
    for m in job_report_errors:
        if "bad_alloc" in m:
            log.warning("encountered a bad_alloc error: %s" % m)
            bad_alloc = True
            diagnostics = m
            break

    return bad_alloc, diagnostics
