#!/usr/bin/env python
import argparse
import os
import re

from pilot.api.data import StageOutClient
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException
from pilot.info import InfoService, FileSpec, infosys
from pilot.util.config import config
from pilot.util.filehandling import establish_logging, write_json
from pilot.util.tracereport import TraceReport

import logging

errors = ErrorCodes()

# error codes
GENERAL_ERROR = 1
NO_QUEUENAME = 2
NO_SCOPES = 3
NO_LFNS = 4
NO_EVENTTYPE = 5
NO_LOCALSITE = 6
NO_REMOTESITE = 7
NO_PRODUSERID = 8
NO_JOBID = 9
NO_TASKID = 10
NO_JOBDEFINITIONID = 11
NO_DDMENDPOINTS = 12
NO_DATASETS = 13
NO_GUIDS = 14
TRANSFER_ERROR = 15


def get_args():
    """
    Return the args from the arg parser.

    :return: args (arg parser object).
    """

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('-d',
                            dest='debug',
                            action='store_true',
                            default=False,
                            help='Enable debug mode for logging messages')
    arg_parser.add_argument('-q',
                            dest='queuename',
                            required=True,
                            help='Queue name (e.g., AGLT2_TEST-condor')
    arg_parser.add_argument('-w',
                            dest='workdir',
                            required=False,
                            default=os.getcwd(),
                            help='Working directory')
    arg_parser.add_argument('--scopes',
                            dest='scopes',
                            required=True,
                            help='List of Rucio scopes (e.g., mc16_13TeV,mc16_13TeV')
    arg_parser.add_argument('--lfns',
                            dest='lfns',
                            required=True,
                            help='LFN list (e.g., filename1,filename2')
    arg_parser.add_argument('--eventtype',
                            dest='eventtype',
                            required=True,
                            help='Event type')
    arg_parser.add_argument('--ddmendpoints',
                            dest='ddmendpoints',
                            required=True,
                            help='DDM endpoint')
    arg_parser.add_argument('--datasets',
                            dest='datasets',
                            required=True,
                            help='Dataset')
    arg_parser.add_argument('--guids',
                            dest='guids',
                            required=True,
                            help='GUIDs')
    arg_parser.add_argument('--localsite',
                            dest='localsite',
                            required=True,
                            help='Local site')
    arg_parser.add_argument('--remotesite',
                            dest='remotesite',
                            required=True,
                            help='Remote site')
    arg_parser.add_argument('--produserid',
                            dest='produserid',
                            required=True,
                            help='produserid')
    arg_parser.add_argument('--jobid',
                            dest='jobid',
                            required=True,
                            help='PanDA job id')
    arg_parser.add_argument('--taskid',
                            dest='taskid',
                            required=True,
                            help='PanDA task id')
    arg_parser.add_argument('--jobdefinitionid',
                            dest='jobdefinitionid',
                            required=True,
                            help='Job definition id')
    arg_parser.add_argument('--eventservicemerge',
                            dest='eventservicemerge',
                            type=str2bool,
                            default=False,
                            help='Event service merge boolean')
    arg_parser.add_argument('--usepcache',
                            dest='usepcache',
                            type=str2bool,
                            default=False,
                            help='pcache boolean from queuedata')
    arg_parser.add_argument('--no-pilot-log',
                            dest='nopilotlog',
                            action='store_true',
                            default=False,
                            help='Do not write the pilot log to file')

    return arg_parser.parse_args()


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


def verify_args():
    """
    Make sure required arguments are set, and if they are not then set them.
    (deprecated)
    :return:
    """
    if not args.workdir:
        args.workdir = os.getcwd()

    if not args.queuename:
        message('queue name not set, cannot initialize InfoService')
        return NO_QUEUENAME

    if not args.scopes:
        message('scopes not set')
        return NO_SCOPES

    if not args.lfns:
        message('LFNs not set')
        return NO_LFNS

    if not args.eventtype:
        message('No event type provided')
        return NO_EVENTTYPE

    if not args.localsite:
        message('No local site provided')
        return NO_LOCALSITE

    if not args.remotesite:
        message('No remote site provided')
        return NO_REMOTESITE

    if not args.produserid:
        message('No produserid provided')
        return NO_PRODUSERID

    if not args.jobid:
        message('No jobid provided')
        return NO_JOBID

    if not args.ddmendpoints:
        message('No ddmendpoint provided')
        return NO_DDMENDPOINTS

    if not args.datasets:
        message('No dataset provided')
        return NO_DATASETS

    if not args.guids:
        message('No GUIDs provided')
        return NO_GUIDS

    if not args.taskid:
        message('No taskid provided')
        return NO_TASKID

    if not args.jobdefinitionid:
        message('No jobdefinitionid provided')
        return NO_JOBDEFINITIONID

    return 0


def message(msg):
    print(msg) if not logger else logger.info(msg)


def get_file_lists(lfns, scopes, ddmendpoints, datasets, guids):
    return lfns.split(','), scopes.split(','), ddmendpoints.split(','), datasets.split(','), guids.split(',')


class Job():
    """
    A minimal implementation of the Pilot Job class with data members necessary for the trace report only.
    """

    produserid = ""
    jobid = ""
    taskid = ""
    jobdefinitionid = ""

    def __init__(self, produserid="", jobid="", taskid="", jobdefinitionid=""):
        self.produserid = produserid.replace('%20', ' ')
        self.jobid = jobid
        self.taskid = taskid
        self.jobdefinitionid = jobdefinitionid


def add_to_dictionary(dictionary, key, value1, value2, value3, value4, value5, value6):
    """
    Add key: [value1, value2, value3, value4, value5, value6] to dictionary.
    In practice; lfn: [status, status_code, surl, turl, checksum, fsize].

    :param dictionary: dictionary to be updated.
    :param key: lfn key to be added (string).
    :param value1: status to be added to list belonging to key (string).
    :param value2: status_code to be added to list belonging to key (string).
    :param value3: surl to be added to list belonging to key (string).
    :param value4: turl to be added to list belonging to key (string).
    :param value5: checksum to be added to list belonging to key (string).
    :param value6: fsize to be added to list belonging to key (string).
    :return: updated dictionary.
    """

    dictionary[key] = [value1, value2, value3, value4, value5, value6]
    return dictionary


def extract_error_info(err):

    error_code = 0
    error_message = ""

    _code = re.search('error code: (\d+)', err)
    if _code:
        error_code = _code.group(1)

    _msg = re.search('details: (.+)', err)
    if _msg:
        error_message = _msg.group(1)
        error_message = error_message.replace('[PilotException(', '').strip()

    return error_code, error_message


if __name__ == '__main__':
    """
    Main function of the stage-in script.
    """

    # get the args from the arg parser
    args = get_args()
    args.debug = True
    args.nopilotlog = False

    establish_logging(args, filename=config.Pilot.stageoutlog)
    logger = logging.getLogger(__name__)

    #ret = verify_args()
    #if ret:
    #    exit(ret)

    # get the file info
    lfns, scopes, ddmendpoints, datasets, guids = get_file_lists(args.lfns, args.scopes, args.ddmendpoints, args.datasets, args.guids)
    if len(lfns) != len(scopes) or len(lfns) != len(ddmendpoints) or len(lfns) != len(datasets) or len(lfns) != len(guids):
        message('file lists not same length: len(lfns)=%d, len(scopes)=%d, len(ddmendpoints)=%d, len(datasets)=%d, len(guids)=%d' %
                (len(lfns), len(scopes), len(ddmendpoints), len(datasets), len(guids)))

    # generate the trace report
    trace_report = TraceReport(pq=os.environ.get('PILOT_SITENAME', ''), localSite=args.localsite,
                               remoteSite=args.remotesite, dataset="", eventType=args.eventtype)
    job = Job(produserid=args.produserid, jobid=args.jobid, taskid=args.taskid, jobdefinitionid=args.jobdefinitionid)
    trace_report.init(job)

    try:
        infoservice = InfoService()
        infoservice.init(args.queuename, infosys.confinfo, infosys.extinfo)
        infosys.init(args.queuename)  # is this correct? otherwise infosys.queuedata doesn't get set
    except Exception as e:
        message(e)

    # perform stage-out (single transfers)
    err = ""
    errcode = 0
    xfiles = None
    activity = 'pw'

    client = StageOutClient(infoservice, logger=logger, trace_report=trace_report)
    kwargs = dict(workdir=args.workdir, cwd=args.workdir, usecontainer=False, job=job)  # , mode='stage-out')

    for lfn, scope, dataset, ddmendpoint, guid in list(zip(lfns, scopes, datasets, ddmendpoints, guids)):
        try:
            files = [{'scope': scope, 'lfn': lfn, 'workdir': args.workdir, 'dataset': dataset, 'ddmendpoint': ddmendpoint, 'ddmendpoint_alt': None}]
            xfiles = [FileSpec(type='output', **f) for f in files]

            # prod analy unification: use destination preferences from PanDA server for unified queues
            if infoservice.queuedata.type != 'unified':
                client.prepare_destinations(xfiles,
                                            activity)  ## FIX ME LATER: split activities: for astorages and for copytools (to unify with ES workflow)

            r = client.transfer(xfiles, activity=activity, **kwargs)
        except PilotException as error:
            import traceback
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            err = errors.format_diagnostics(error.get_error_code(), error_msg)
        except Exception as error:
            err = str(error)
            errcode = -1
            message(err)

    # put file statuses in a dictionary to be written to file
    file_dictionary = {}  # { 'error': [error_diag, -1], 'lfn1': [status, status_code], 'lfn2':.., .. }
    if xfiles:
        message('stageout script summary of transferred files:')
        for fspec in xfiles:
            add_to_dictionary(file_dictionary, fspec.lfn, fspec.status, fspec.status_code, fspec.surl, fspec.turl, fspec.checksum.get('adler32'), fspec.filesize)
            status = fspec.status if fspec.status else "(not transferred)"
            message(" -- lfn=%s, status_code=%s, status=%s, surl=%s, turl=%s, checksum=%s, filesize=%s" %
                    (fspec.lfn, fspec.status_code, status, fspec.surl, fspec.turl, fspec.checksum.get('adler32'), fspec.filesize))

    # add error info, if any
    if err:
        errcode, err = extract_error_info(err)
    add_to_dictionary(file_dictionary, 'error', err, errcode, None, None, None, None)
    path = os.path.join(args.workdir, config.Container.stageout_dictionary)
    if os.path.exists(path):
        file_dictionary += '.log'
    _status = write_json(path, file_dictionary)
    if err:
        message("containerised file transfers failed: %s" % err)
        exit(TRANSFER_ERROR)

    message("wrote %s" % path)
    message("containerised file transfers finished")
    exit(0)
