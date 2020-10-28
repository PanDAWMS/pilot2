# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020

import argparse
import os
import logging
import ROOT

from pilot.util.config import config
from pilot.util.filehandling import establish_logging, write_json

logger = logging.getLogger(__name__)


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
    arg_parser.add_argument('-w',
                            dest='workdir',
                            required=False,
                            default=os.getcwd(),
                            help='Working directory')
    arg_parser.add_argument('--turls',
                            dest='turls',
                            required=True,
                            help='TURL list (e.g., filepath1,filepath2')
    arg_parser.add_argument('--no-pilot-log',
                            dest='nopilotlog',
                            action='store_true',
                            default=False,
                            help='Do not write the pilot log to file')

    return arg_parser.parse_args()


def message(msg):
    print(msg) if not logger else logger.info(msg)


def get_file_lists(turls):
    _turls = []

    try:
        _turls = turls.split(',')
    except Exception as error:
        message("exception caught: %s" % error)

    return {'turls': _turls}


def try_open_file(turl):
    turl_opened = False
    try:
        in_file = ROOT.TFile.Open(turl)
    except Exception as error:
        message('caught exception: %s' % error)
    else:
        if in_file and in_file.IsOpen():
            in_file.Close()
            turl_opened = True

    return turl_opened


if __name__ == '__main__':
    """
    Main function of the remote file open script.
    """

    # get the args from the arg parser
    args = get_args()
    args.debug = True
    args.nopilotlog = False

    logname = 'default.log'
    try:
        logname = config.Pilot.remotefileverification_log
    except Exception as error:
        print("caught exception: %s (skipping remote file open verification)" % error)
        exit(1)
    else:
        if not logname:
            print("remote file open verification not desired")
            exit(0)

    establish_logging(args, filename=logname)
    logger = logging.getLogger(__name__)

    # get the file info
    file_list_dictionary = get_file_lists(args.turls)
    turls = file_list_dictionary.get('turls')
    processed_turls_dictionary = {}
    if turls:
        message('got TURLs: %s' % str(turls))
        for turl in turls:
            processed_turls_dictionary[turl] = try_open_file(turl)

        # write dictionary to file with results
        _status = write_json(os.path.join(args.workdir, config.Pilot.remotefileverification_dictionary), processed_turls_dictionary)
    else:
        message('no TURLs to verify')

    exit(0)
