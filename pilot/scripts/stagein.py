import argparse
import os

from pilot.api import data
from pilot.info import InfoService, FileSpec, infosys
from pilot.util.filehandling import establish_logging, read_json

import logging

logger = logging.getLogger(__name__)

# error codes
GENERAL_ERROR = 1
NO_QUEUENAME = 2
NO_SCOPES = 3
NO_LFNS = 4
NO_TRACEREPORTNAME = 5
NO_TRACEREPORT = 6
TRANSFER_ERROR = 7


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
    arg_parser.add_argument('--tracereportname',
                            dest='tracereportname',
                            required=True,
                            help='Trace report file name')
    arg_parser.add_argument('--no-pilot-log',
                            dest='nopilotlog',
                            action='store_true',
                            default=False,
                            help='Do not write the pilot log to file')

    return arg_parser.parse_args()


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

    if not args.tracereportname:
        message('No trace report file name provided')
        return NO_TRACEREPORTNAME

    return 0


def message(msg):
    print(msg) if not logger else logger.fatal(msg)


def get_file_lists(lfns, scopes):
    return lfns.split(','), scopes.split(',')


if __name__ == '__main__':
    """
    Main function of the stage-in script.
    """

    # get the args from the arg parser
    args = get_args()
    establish_logging(args)
    #ret = verify_args()
    #if ret:
    #    exit(ret)

    # get the file info
    lfns, scopes = get_file_lists(args.lfns, args.scopes)
    if len(lfns) != len(scopes):
        message('file lists not same length: len(lfns)=%d, len(scopes)=%d' % (len(lfns), len(scopes)))

    # get the initial trace report
    path = os.path.join(args.workdir, args.tracereportname)
    if not os.path.exists(path):
        message('file does not exist: %s' % path)
        exit(NO_TRACEREPORT)

    trace_report = read_json(args.tracereportname)
    if not trace_report:
        message('failed to read trace report')
        exit(NO_TRACEREPORT)

    try:
        infoservice = InfoService()
        infoservice.init(args.queuename, infosys.confinfo, infosys.extinfo)
        infosys.init(args.queuename)  # is this correct? otherwise infosys.queuedata doesn't get set
    except Exception as e:
        message(e)

    # perform stage-in (single transfers)
    err = ""
    for lfn, scope in list(zip(lfns, scopes)):
        try:
            client = data.StageInClient(infoservice, logger=logger, trace_report=trace_report)
            files = [{'scope': scope, 'lfn': lfn, 'workdir': args.workdir}]
            xfiles = [FileSpec(type='input', **f) for f in files]
            r = client.transfer(xfiles)
        except Exception as e:
            err = str(e)
            message(err)
            # break
    if err:
        message("file transfer failed: %s" % err)
        exit(TRANSFER_ERROR)

    message("file transfers finished")
    exit(0)
