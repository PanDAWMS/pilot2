#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019
# - Danila Oleynik danila.oleynik@cern.ch, 2018

import functools
import logging
import os
import signal
import time
from collections import namedtuple
from datetime import datetime

try:
    from functools import reduce  # Python 3
except Exception:
    pass

from pilot.common.exception import FileHandlingFailure
from pilot.util.auxiliary import set_pilot_state
from pilot.util.config import config
from pilot.util.constants import SUCCESS, FAILURE, PILOT_PRE_GETJOB, PILOT_POST_GETJOB, PILOT_PRE_SETUP, \
    PILOT_POST_SETUP, PILOT_PRE_PAYLOAD, PILOT_POST_PAYLOAD, PILOT_PRE_STAGEOUT, PILOT_POST_STAGEOUT, PILOT_PRE_FINAL_UPDATE, PILOT_POST_FINAL_UPDATE
from pilot.util.container import execute
from pilot.util.filehandling import tar_files, write_json, read_json, copy
from pilot.util.harvester import get_initial_work_report, publish_work_report
from pilot.util.timing import add_to_pilot_timing

logger = logging.getLogger(__name__)


def interrupt(args, signum, frame):
    """
    Interrupt function on the receiving end of kill signals.
    This function is forwarded any incoming signals (SIGINT, SIGTERM, etc) and will set abort_job which instructs
    the threads to abort the job.

    :param args: pilot arguments.
    :param signum: signal.
    :param frame: stack/execution frame pointing to the frame that was interrupted by the signal.
    :return:
    """

    try:
        logger.info('caught signal: %s' % [v for v, k in signal.__dict__.iteritems() if k == signum][0])  # Python 2
    except Exception:
        logger.info('caught signal: %s' % [v for v, k in list(signal.__dict__.items()) if k == signum][0])  # Python 3

    args.graceful_stop.set()


def run(args):
    """
    Main execution function for the generic HPC workflow.

    :param args: pilot arguments.
    :returns: traces object.
    """

    # set communication point. Worker report should be placed there, matched with working directory of Harvester
    if args.harvester_workdir:
        communication_point = args.harvester_workdir
    else:
        communication_point = os.getcwd()
    work_report = get_initial_work_report()
    worker_attributes_file = config.Harvester.workerAttributesFile
    worker_stageout_declaration = config.Harvester.StageOutnFile
    payload_report_file = config.Payload.jobreport
    payload_stdout_file = config.Payload.payloadstdout
    payload_stderr_file = config.Payload.payloadstderr

    try:
        logger.info('setting up signal handling')
        signal.signal(signal.SIGINT, functools.partial(interrupt, args))

        logger.info('setting up tracing')
        traces = namedtuple('traces', ['pilot'])
        traces.pilot = {'state': SUCCESS,
                        'nr_jobs': 0}

        if args.hpc_resource == '':
            logger.critical('hpc resource not specified, cannot continue')
            traces.pilot['state'] = FAILURE
            return traces

        # get the resource reference
        resource = __import__('pilot.resource.%s' % args.hpc_resource, globals(), locals(), [args.hpc_resource], 0)  # Python 2/3

        # get the user reference
        user = __import__('pilot.user.%s.common' % args.pilot_user.lower(), globals(), locals(),
                          [args.pilot_user.lower()], 0)  # Python 2/3

        # get job (and rank)
        add_to_pilot_timing('0', PILOT_PRE_GETJOB, time.time(), args)
        job, rank = resource.get_job(communication_point)
        add_to_pilot_timing(job.jobid, PILOT_POST_GETJOB, time.time(), args)
        # cd to job working directory

        add_to_pilot_timing(job.jobid, PILOT_PRE_SETUP, time.time(), args)
        work_dir = resource.set_job_workdir(job, communication_point)
        work_report['workdir'] = work_dir
        worker_attributes_file = os.path.join(work_dir, worker_attributes_file)
        logger.debug("Worker attributes will be publeshied in: {0}".format(worker_attributes_file))

        set_pilot_state(job=job, state="starting")
        work_report["jobStatus"] = job.state
        publish_work_report(work_report, worker_attributes_file)

        # Get HPC specific setup commands
        logger.info('setup for resource %s: %s' % (args.hpc_resource, str(resource.get_setup())))
        setup_str = "; ".join(resource.get_setup())

        # Prepare job scratch directory (RAM disk etc.)
        job_scratch_dir = resource.set_scratch_workdir(job, work_dir, args)

        my_command = " ".join([job.script, job.script_parameters])
        my_command = resource.command_fix(my_command, job_scratch_dir)
        my_command = setup_str + my_command
        add_to_pilot_timing(job.jobid, PILOT_POST_SETUP, time.time(), args)

        # Basic execution. Should be replaced with something like 'run_payload'
        logger.debug("Going to launch: {0}".format(my_command))
        logger.debug("Current work directory: {0}".format(job_scratch_dir))
        payloadstdout = open(payload_stdout_file, "w")
        payloadstderr = open(payload_stderr_file, "w")

        add_to_pilot_timing(job.jobid, PILOT_PRE_PAYLOAD, time.time(), args)
        set_pilot_state(job=job, state="running")
        work_report["jobStatus"] = job.state
        work_report["startTime"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        start_time = time.asctime(time.localtime(time.time()))
        job.startTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        publish_work_report(work_report, worker_attributes_file)

        stime = time.time()
        t0 = os.times()
        exit_code, stdout, stderr = execute(my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True)
        logger.debug("Payload exit code: {0}".format(exit_code))
        t1 = os.times()
        exetime = time.time() - stime
        end_time = time.asctime(time.localtime(time.time()))
        t = list(map(lambda x, y: x - y, t1, t0))  # Python 2/3
        t_tot = reduce(lambda x, y: x + y, t[2:3])
        job.endTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        payloadstdout.close()
        payloadstderr.close()
        add_to_pilot_timing(job.jobid, PILOT_POST_PAYLOAD, time.time(), args)

        state = 'finished' if exit_code == 0 else 'failed'
        set_pilot_state(job=job, state=state)
        job.exitcode = exit_code

        work_report["startTime"] = job.startTime
        work_report["endTime"] = job.endTime
        work_report["jobStatus"] = job.state
        work_report["cpuConsumptionTime"] = t_tot
        work_report["transExitCode"] = job.exitcode

        log_jobreport = "\nPayload exit code: {0} JobID: {1} \n".format(exit_code, job.jobid)
        log_jobreport += "CPU comsumption time: {0}  JobID: {1} \n".format(t_tot, job.jobid)
        log_jobreport += "Start time: {0}  JobID: {1} \n".format(start_time, job.jobid)
        log_jobreport += "End time: {0}  JobID: {1} \n".format(end_time, job.jobid)
        log_jobreport += "Execution time: {0} sec.  JobID: {1} \n".format(exetime, job.jobid)
        logger.info(log_jobreport)
        log_jobreport = "\nJob report start time: {0}\nJob report end time: {1}".format(job.startTime, job.endTime)
        logger.debug(log_jobreport)

        # Parse job report file and update of work report
        if os.path.exists(payload_report_file):
            payload_report = user.parse_jobreport_data(read_json(payload_report_file))
            work_report.update(payload_report)
            resource.process_jobreport(payload_report_file, job_scratch_dir, work_dir)

        resource.postprocess_workdir(job_scratch_dir)

        # output files should not be packed with logs
        protectedfiles = list(job.output_files.keys())  # Python 2/3

        # log file not produced (yet), so should be excluded
        if job.log_file in protectedfiles:
            protectedfiles.remove(job.log_file)
        else:
            logger.info("Log files was not declared")

        logger.info("Cleanup of working directory")

        protectedfiles.extend([worker_attributes_file, worker_stageout_declaration])
        user.remove_redundant_files(job_scratch_dir, protectedfiles)
        res = tar_files(job_scratch_dir, protectedfiles, job.log_file)
        if res > 0:
            raise FileHandlingFailure("Log file tar failed")

        add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEOUT, time.time(), args)
        # Copy of output to shared FS for stageout
        if not job_scratch_dir == work_dir:
            copy_output(job, job_scratch_dir, work_dir)
        add_to_pilot_timing(job.jobid, PILOT_POST_STAGEOUT, time.time(), args)

        logger.info("Declare stage-out")
        add_to_pilot_timing(job.jobid, PILOT_PRE_FINAL_UPDATE, time.time(), args)
        declare_output(job, work_report, worker_stageout_declaration)

        logger.info("All done")
        publish_work_report(work_report, worker_attributes_file)
        traces.pilot['state'] = SUCCESS
        logger.debug("Final report: {0}".format(work_report))
        add_to_pilot_timing(job.jobid, PILOT_POST_FINAL_UPDATE, time.time(), args)

    except Exception as e:
        work_report["jobStatus"] = "failed"
        work_report["exitMsg"] = str(e)
        publish_work_report(work_report, worker_attributes_file)
        logging.exception('exception caught:')
        traces.pilot['state'] = FAILURE

    return traces


def copy_output(job, job_scratch_dir, work_dir):
    cp_start = time.time()
    try:
        for outfile in list(job.output_files.keys()):  # Python 2/3
            if os.path.exists(outfile):
                copy(os.path.join(job_scratch_dir, outfile), os.path.join(work_dir, outfile))
        os.chdir(work_dir)
    except IOError:
        raise FileHandlingFailure("Copy from scratch dir to access point failed")
    finally:
        cp_time = time.time() - cp_start
        logger.info("Copy of outputs took: {0} sec.".format(cp_time))
    return 0


def declare_output(job, work_report, worker_stageout_declaration):
    out_file_report = {}
    out_file_report[job.jobid] = []
    for outfile in list(job.output_files.keys()):  # Python 2/3
        logger.debug("File {} will be checked and declared for stage out".format(outfile))
        if os.path.exists(outfile):
            file_desc = {}
            if outfile == job.log_file:
                file_desc['filetype'] = 'log'
            else:
                file_desc['filetype'] = 'output'
            file_desc['path'] = os.path.abspath(outfile)
            file_desc['fsize'] = os.path.getsize(outfile)
            if 'guid' in list(job.output_files[outfile].keys()):  # Python 2/3
                file_desc['guid'] = job.output_files[outfile]['guid']
            elif work_report['outputfiles'] and work_report['outputfiles'][outfile]:
                file_desc['guid'] = work_report['outputfiles'][outfile]['guid']
            out_file_report[job.jobid].append(file_desc)
        else:
            logger.info("Expected output file {0} missed. Job {1} will be failed".format(outfile, job.jobid))
            set_pilot_state(job=job, state='failed')

    if out_file_report[job.jobid]:
        write_json(worker_stageout_declaration, out_file_report)
        logger.debug('Stagout declared in: {0}'.format(worker_stageout_declaration))
        logger.debug('Report for stageout: {}'.format(out_file_report))
