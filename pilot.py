#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019

from __future__ import print_function  # Python 2 (2to3 complains about this)

import argparse
import logging
import sys
import threading
import time
from os import getcwd, chdir, environ
from os.path import exists, join
from shutil import rmtree

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException
from pilot.info import infosys
from pilot.util.auxiliary import pilot_version_banner, shell_exit_code
from pilot.util.constants import SUCCESS, FAILURE, ERRNO_NOJOBS, PILOT_START_TIME, PILOT_END_TIME, get_pilot_version, \
    SERVER_UPDATE_NOT_DONE, PILOT_MULTIJOB_START_TIME
from pilot.util.filehandling import get_pilot_work_dir, mkdirs, establish_logging
from pilot.util.harvester import is_harvester_mode
from pilot.util.https import https_setup
from pilot.util.timing import add_to_pilot_timing

errors = ErrorCodes()


def main():
    """
    Main function of PanDA Pilot 2.
    Prepare for and execute the requested workflow.

    :return: exit code (int).
    """

    # get the logger
    logger = logging.getLogger(__name__)

    # print the pilot version
    pilot_version_banner()

    # define threading events
    args.graceful_stop = threading.Event()
    args.abort_job = threading.Event()
    args.job_aborted = threading.Event()

    # define useful variables
    args.retrieve_next_job = True  # go ahead and download a new job
    args.signal = None  # to store any incoming signals
    args.signal_counter = 0  # keep track of number of received kill signal (suicide counter)
    args.kill_time = 0  # keep track of when first kill signal arrived

    # perform https setup
    if args.use_https:
        https_setup(args, get_pilot_version())

    # initialize InfoService
    try:
        infosys.init(args.queue)
        # check if queue is ACTIVE
        if infosys.queuedata.state != 'ACTIVE':
            logger.critical('specified queue is NOT ACTIVE: %s -- aborting' % infosys.queuedata.name)
            return errors.PANDAQUEUENOTACTIVE
    except PilotException as error:
        logger.fatal(error)
        return error.get_error_code()

    # set the site name for rucio  ## is it really used?
    environ['PILOT_RUCIO_SITENAME'] = infosys.queuedata.site

    # store the site name as set with a pilot option
    environ['PILOT_SITENAME'] = infosys.queuedata.resource  #args.site  # TODO: replace with singleton

    # set requested workflow
    logger.info('pilot arguments: %s' % str(args))
    workflow = __import__('pilot.workflow.%s' % args.workflow, globals(), locals(), [args.workflow], 0)  # Python 3, -1 -> 0

    # execute workflow
    try:
        exit_code = workflow.run(args)
    except Exception as e:
        logger.fatal('main pilot function caught exception: %s' % e)
        exit_code = None

    return exit_code


class Args:
    """
    Dummy namespace class used to contain pilot arguments.
    """
    pass


# rename module to pilot2 to avoid conflict in import with pilot directory
def import_module(**kwargs):
    """
    This function allows for importing the pilot code.

    :param kwargs: pilot options (dictionary).
    :return: pilot error code (integer).
    """

    argument_dictionary = {'-a': kwargs.get('workdir', ''),
                           '-d': kwargs.get('debug', None),
                           '-w': kwargs.get('workflow', 'generic'),
                           '-l': kwargs.get('lifetime', '3600'),
                           '-q': kwargs.get('queue'),  # required
                           '-r': kwargs.get('resource'),  # required
                           '-s': kwargs.get('site'),  # required
                           '-j': kwargs.get('job_label', 'ptest'),  # change default later to 'managed'
                           '-i': kwargs.get('version_tag', 'PR'),
                           '-t': kwargs.get('verify_proxy', True),
                           '-z': kwargs.get('update_server', True),
                           '--cacert': kwargs.get('cacert', None),
                           '--capath': kwargs.get('capath'),
                           '--url': kwargs.get('url', ''),
                           '-p': kwargs.get('port', '25443'),
                           '--country-group': kwargs.get('country_group', ''),
                           '--working-group': kwargs.get('working_group', ''),
                           '--allow-other-country': kwargs.get('allow_other_country', 'False'),
                           '--allow-same-user': kwargs.get('allow_same_user', 'True'),
                           '--pilot-user': kwargs.get('pilot_user', 'generic'),
                           '--input-dir': kwargs.get('input_dir', ''),
                           '--output-dir': kwargs.get('output_dir', ''),
                           '--hpc-resource': kwargs.get('hpc_resource', ''),
                           '--harvester-workdir': kwargs.get('harvester_workdir', ''),
                           '--harvester-datadir': kwargs.get('harvester_datadir', ''),
                           '--harvester-eventstatusdump': kwargs.get('harvester_eventstatusdump', ''),
                           '--harvester-workerattributes': kwargs.get('harvester_workerattributes', ''),
                           '--harvester-submitmode': kwargs.get('harvester_submitmode', ''),
                           '--resource-type': kwargs.get('resource_type', '')
                           }

    args = Args()
    parser = argparse.ArgumentParser()
    try:
        _items = list(argument_dictionary.items())  # Python 3
    except Exception:
        _items = argument_dictionary.iteritems()  # Python 2
    for key, value in _items:
        print(key, value)
        parser.add_argument(key)
        parser.parse_args(args=[key, value], namespace=args)  # convert back int and bool strings to int and bool??

    # call main pilot function

    return 0


def str2bool(v):
    """ Helper function to convert string to bool """

    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def get_args():
    """
    Return the args from the arg parser.

    :return: args (arg parser object).
    """

    arg_parser = argparse.ArgumentParser()

    # pilot log creation
    arg_parser.add_argument('--no-pilot-log',
                            dest='nopilotlog',
                            action='store_true',
                            default=False,
                            help='Do not write the pilot log to file')

    # pilot work directory
    arg_parser.add_argument('-a',
                            dest='workdir',
                            default="",
                            help='Pilot work directory')

    # debug option to enable more log messages
    arg_parser.add_argument('-d',
                            dest='debug',
                            action='store_true',
                            default=False,
                            help='Enable debug mode for logging messages')

    # the choices must match in name the python module in pilot/workflow/
    arg_parser.add_argument('-w',
                            dest='workflow',
                            default='generic',
                            choices=['generic', 'generic_hpc',
                                     'production', 'production_hpc',
                                     'analysis', 'analysis_hpc',
                                     'eventservice_hpc', 'stagein', 'payload_stageout'],
                            help='Pilot workflow (default: generic)')

    # graciously stop pilot process after hard limit
    arg_parser.add_argument('-l',
                            dest='lifetime',
                            default=324000,
                            required=False,
                            type=int,
                            help='Pilot lifetime seconds (default: 324000 s)')

    # set the appropriate site, resource and queue
    arg_parser.add_argument('-q',
                            dest='queue',
                            required=True,
                            help='MANDATORY: queue name (e.g., AGLT2_TEST-condor')
    arg_parser.add_argument('-r',
                            dest='resource',
                            required=False,  # From v 2.2.0 the resource name is internally set
                            help='OBSOLETE: resource name (e.g., AGLT2_TEST')
    arg_parser.add_argument('-s',
                            dest='site',
                            required=False,  # From v 2.2.1 the site name is internally set
                            help='OBSOLETE: site name (e.g., AGLT2_TEST')

    # graciously stop pilot process after hard limit
    arg_parser.add_argument('-j',
                            dest='job_label',
                            default='ptest',
                            help='Job prod/source label (default: ptest)')

    # pilot version tag; PR or RC
    arg_parser.add_argument('-i',
                            dest='version_tag',
                            default='PR',
                            help='Version tag (default: PR, optional: RC)')

    arg_parser.add_argument('-z',
                            dest='update_server',
                            action='store_false',
                            default=True,
                            help='Disable server updates')

    arg_parser.add_argument('-t',
                            dest='verify_proxy',
                            action='store_false',
                            default=True,
                            help='Disable proxy verification')

    # SSL certificates
    arg_parser.add_argument('--cacert',
                            dest='cacert',
                            default=None,
                            help='CA certificate to use with HTTPS calls to server, commonly X509 proxy',
                            metavar='path/to/your/certificate')
    arg_parser.add_argument('--capath',
                            dest='capath',
                            default=None,
                            help='CA certificates path',
                            metavar='path/to/certificates/')

    # Server URLs and ports
    arg_parser.add_argument('--url',
                            dest='url',
                            default='',  # the proper default is stored in config.cfg
                            help='PanDA server URL')
    arg_parser.add_argument('-p',
                            dest='port',
                            default=25443,
                            help='PanDA server port')
    arg_parser.add_argument('--queuedata-url',
                            dest='queuedata_url',
                            default='',
                            help='Queuedata server URL')

    # Country group
    arg_parser.add_argument('--country-group',
                            dest='country_group',
                            default='',
                            help='Country group option for getjob request')

    # Working group
    arg_parser.add_argument('--working-group',
                            dest='working_group',
                            default='',
                            help='Working group option for getjob request')

    # Allow other country
    arg_parser.add_argument('--allow-other-country',
                            dest='allow_other_country',
                            type=str2bool,
                            default=False,
                            help='Is the resource allowed to be used outside the privileged group?')

    # Allow same user
    arg_parser.add_argument('--allow-same-user',
                            dest='allow_same_user',
                            type=str2bool,
                            default=True,
                            help='Multi-jobs will only come from same taskID (and thus same user)')

    # Experiment
    arg_parser.add_argument('--pilot-user',
                            dest='pilot_user',
                            default='generic',
                            required=True,
                            help='Pilot user, e.g. name of experiment')

    # Harvester specific options (if any of the following options are used, args.harvester will be set to True)
    arg_parser.add_argument('--harvester-workdir',
                            dest='harvester_workdir',
                            default='',
                            help='Harvester work directory')
    arg_parser.add_argument('--harvester-datadir',
                            dest='harvester_datadir',
                            default='',
                            help='Harvester data directory')
    arg_parser.add_argument('--harvester-eventstatusdump',
                            dest='harvester_eventstatusdump',
                            default='',
                            help='Harvester event status dump json file containing processing status')
    arg_parser.add_argument('--harvester-workerattributes',
                            dest='harvester_workerattributes',
                            default='',
                            help='Harvester worker attributes json file containing job status')
    arg_parser.add_argument('--harvester-submit-mode',
                            dest='harvester_submitmode',
                            default='PULL',
                            help='Harvester submit mode (PUSH or PULL [default])')
    arg_parser.add_argument('--resource-type',
                            dest='resource_type',
                            default='',
                            type=str,
                            choices=['SCORE', 'MCORE', 'SCORE_HIMEM', 'MCORE_HIMEM'],
                            help='Resource type; MCORE, SCORE, SCORE_HIMEM or MCORE_HIMEM')
    arg_parser.add_argument('--use-https',
                            dest='use_https',
                            type=str2bool,
                            default=True,
                            help='Use HTTPS protocol for communications with server')
    arg_parser.add_argument('--cleanup',
                            dest='cleanup',
                            type=str2bool,
                            default=True,
                            help='Cleanup work directory after pilot has finished')

    # Harvester and Nordugrid specific options
    arg_parser.add_argument('--input-dir',
                            dest='input_dir',
                            default='',
                            help='Input directory')
    arg_parser.add_argument('--output-dir',
                            dest='output_dir',
                            default='',
                            help='Output directory')
    arg_parser.add_argument('--job-type',
                            dest='jobtype',
                            default='',
                            help='Job type (managed, user)')

    # HPC options
    arg_parser.add_argument('--hpc-resource',
                            dest='hpc_resource',
                            default='',
                            help='Name of the HPC (e.g. Titan)')
    arg_parser.add_argument('--hpc-mode',
                            dest='hpc_mode',
                            default='manytoone',
                            help='HPC mode (manytoone, jumbojobs)')

    return arg_parser.parse_args()


def create_main_work_dir(args):
    """
    Create and return the pilot's main work directory.
    The function also sets args.mainworkdir and cd's into this directory.

    :param args: pilot arguments object.
    :return: exit code (int), main work directory (string).
    """

    exit_code = 0

    if args.workdir != "":
        mainworkdir = get_pilot_work_dir(args.workdir)
        try:
            # create the main PanDA Pilot work directory
            mkdirs(mainworkdir)
        except Exception as e:
            # print to stderr since logging has not been established yet
            print('failed to create workdir at %s -- aborting: %s' % (mainworkdir, e), file=sys.stderr)
            exit_code = shell_exit_code(e._errorCode)
    else:
        mainworkdir = getcwd()

    args.mainworkdir = mainworkdir
    chdir(mainworkdir)

    return exit_code, mainworkdir


def set_environment_variables(args, mainworkdir):
    """
    Set environment variables. To be replaced with singleton implementation.
    This function sets PILOT_WORK_DIR, PILOT_HOME, PILOT_SITENAME, PILOT_USER and PILOT_VERSION and others.

    :param args: args object.
    :param mainworkdir: work directory (string).
    :return:
    """

    # working directory as set with a pilot option (e.g. ..)
    environ['PILOT_WORK_DIR'] = args.workdir  # TODO: replace with singleton

    # main work directory (e.g. /scratch/PanDA_Pilot2_3908_1537173670)
    environ['PILOT_HOME'] = mainworkdir  # TODO: replace with singleton

    # pilot source directory (e.g. /cluster/home/usatlas1/gram_scratch_hHq4Ns/condorg_oqmHdWxz)
    environ['PILOT_SOURCE_DIR'] = args.sourcedir  # TODO: replace with singleton

    # set the pilot user (e.g. ATLAS)
    environ['PILOT_USER'] = args.pilot_user  # TODO: replace with singleton

    # internal pilot state
    environ['PILOT_JOB_STATE'] = 'startup'  # TODO: replace with singleton

    # set the pilot version
    environ['PILOT_VERSION'] = get_pilot_version()

    # set the default wrap-up/finish instruction
    environ['PILOT_WRAP_UP'] = 'NORMAL'

    # proxy verification
    environ['PILOT_PROXY_VERIFICATION'] = '%s' % args.verify_proxy

    # keep track of the server updates, if any
    environ['SERVER_UPDATE'] = SERVER_UPDATE_NOT_DONE

    # set the (HPC) resource name (if set in options)
    environ['PILOT_RESOURCE_NAME'] = args.hpc_resource

    # keep track of the PanDA server url
    environ['QUEUEDATA_SERVER_URL'] = '%s' % args.queuedata_url


def wrap_up(initdir, mainworkdir, args):
    """
    Perform cleanup and terminate logging.

    :param initdir: launch directory (string).
    :param mainworkdir: main work directory (string).
    :param args: pilot arguments object.
    :return: exit code (int).
    """

    exit_code = 0

    # cleanup pilot workdir if created
    if initdir != mainworkdir and args.cleanup:
        chdir(initdir)
        try:
            rmtree(mainworkdir)
        except Exception as e:
            logging.warning("failed to remove %s: %s" % (mainworkdir, e))
        else:
            logging.info("removed %s" % mainworkdir)

    # in Harvester mode, create a kill_worker file that will instruct Harvester that the pilot has finished
    if args.harvester:
        from pilot.util.harvester import kill_worker
        kill_worker()

    try:
        exit_code = trace.pilot['error_code']
    except Exception:
        exit_code = trace
    else:
        logging.info('traces error code: %d' % exit_code)
        if trace.pilot['nr_jobs'] <= 1:
            if exit_code != 0:
                logging.info('an exit code was already set: %d (will be converted to a standard shell code)' % exit_code)
        elif trace.pilot['nr_jobs'] > 0:
            if trace.pilot['nr_jobs'] == 1:
                logging.getLogger(__name__).info('pilot has finished (%d job was processed)' % trace.pilot['nr_jobs'])
            else:
                logging.getLogger(__name__).info('pilot has finished (%d jobs were processed)' % trace.pilot['nr_jobs'])
            exit_code = SUCCESS
        elif trace.pilot['state'] == FAILURE:
            logging.critical('pilot workflow failure -- aborting')
        elif trace.pilot['state'] == ERRNO_NOJOBS:
            logging.critical('pilot did not process any events -- aborting')
            exit_code = ERRNO_NOJOBS
    logging.info('pilot has finished')
    logging.shutdown()

    return shell_exit_code(exit_code)


def get_pilot_source_dir():
    """
    Return the pilot source directory.

    :return: full path to pilot source directory.
    """

    cwd = getcwd()
    if exists(join(join(cwd, 'pilot2'), 'pilot.py')):  # in case wrapper has untarred src as pilot2 in init dir
        return join(cwd, 'pilot2')
    elif exists(join(cwd, 'pilot.py')):  # in case pilot gets launched from within the src dir
        return cwd
    else:
        # could throw error here, but logging is not setup yet - fail later
        return cwd


if __name__ == '__main__':
    """
    Main function of pilot module.
    """

    # get the args from the arg parser
    args = get_args()

    # Define and set the main harvester control boolean
    args.harvester = is_harvester_mode(args)

    # initialize the pilot timing dictionary
    args.timing = {}  # TODO: move to singleton?

    # initialize job status dictionary (e.g. used to keep track of log transfers)
    args.job_status = {}  # TODO: move to singleton or to job object directly?

    # store T0 time stamp
    add_to_pilot_timing('0', PILOT_START_TIME, time.time(), args)
    add_to_pilot_timing('1', PILOT_MULTIJOB_START_TIME, time.time(), args)

    # if requested by the wrapper via a pilot option, create the main pilot workdir and cd into it
    args.sourcedir = getcwd()  #get_pilot_source_dir()

    exit_code, mainworkdir = create_main_work_dir(args)
    if exit_code != 0:
        sys.exit(exit_code)

    # set environment variables (to be replaced with singleton implementation)
    set_environment_variables(args, mainworkdir)

    # setup and establish standard logging
    establish_logging(args)

    # execute main function
    trace = main()

    # store final time stamp (cannot be placed later since the mainworkdir is about to be purged)
    add_to_pilot_timing('0', PILOT_END_TIME, time.time(), args, store=False)

    # perform cleanup and terminate logging
    exit_code = wrap_up(args.sourcedir, mainworkdir, args)

    # the end.
    sys.exit(exit_code)
