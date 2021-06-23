# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-2021

import argparse
import os
import logging
import threading
import queue
import ROOT
from collections import namedtuple

from pilot.util.config import config
from pilot.util.filehandling import (
    establish_logging,
    write_json,
)

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
    arg_parser.add_argument('-t',
                            dest='nthreads',
                            default=1,
                            required=False,
                            type=int,
                            help='Number of concurrent file open threads')
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
    """
    Print message to stdout or to log.
    Note: not using lazy formatting.

    :param msg: message (string).
    :return:
    """

    print(msg) if not logger else logger.info(msg)


def get_file_lists(turls):
    """
    Return a dictionary with the turls.
    Format: {'turls': <turl list>}

    :param turls: comma separated turls (string)
    :return: turls dictionary.
    """

    _turls = []

    try:
        _turls = turls.split(',')
    except Exception as error:
        message("exception caught: %s" % error)

    return {'turls': _turls}


def try_open_file(turl, queues):
    """
    Attempt to open a remote file.
    Successfully opened turls will be put in the queues.opened queue. Unsuccessful turls will be placed in
    the queues.unopened queue.

    :param turl: turl (string).
    :param queues: queues collection.
    :return:
    """

    turl_opened = False
    try:
        in_file = ROOT.TFile.Open(turl)
    except Exception as error:
        message('caught exception: %s' % error)
    else:
        if in_file and in_file.IsOpen():
            in_file.Close()
            turl_opened = True

    queues.opened.put(turl) if turl_opened else queues.unopened.put(turl)


def spawn_file_open_thread(queues, file_list):
    """
    Spawn a thread for the try_open_file().

    :param queues: queue collection.
    :param file_list: files to open (list).
    :return: thread.
    """

    thread = None
    try:
        turl = file_list.pop(0)
    except IndexError:
        pass
    else:
        # create and start thread for the current turl
        thread = threading.Thread(target=try_open_file, args=(turl, queues))
        thread.start()

    return thread


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

    establish_logging(debug=args.debug, nopilotlog=args.nopilotlog, filename=logname)
    logger = logging.getLogger(__name__)

    # get the file info
    file_list_dictionary = get_file_lists(args.turls)
    turls = file_list_dictionary.get('turls')
    processed_turls_dictionary = {}

    queues = namedtuple('queues', ['result', 'opened', 'unopened'])
    queues.result = queue.Queue()
    queues.opened = queue.Queue()
    queues.unopened = queue.Queue()
    threads = []

    if turls:
        # make N calls to begin with
        for index in range(args.nthreads):
            thread = spawn_file_open_thread(queues, turls)
            if thread:
                threads.append(thread)

        while turls:

            try:
                _ = queues.result.get(block=True)
            except Exception as error:
                message("caught exception: %s" % error)

            thread = spawn_file_open_thread(queues, turls)
            if thread:
                threads.append(thread)

        # wait until all threads have finished
        [_thread.join() for _thread in threads]

        opened_turls = list(queues.opened.queue)
        opened_turls.sort()
        unopened_turls = list(queues.unopened.queue)
        unopened_turls.sort()

        for turl in opened_turls:
            processed_turls_dictionary[turl] = True
        for turl in unopened_turls:
            processed_turls_dictionary[turl] = False

        # write dictionary to file with results
        _status = write_json(os.path.join(args.workdir, config.Pilot.remotefileverification_dictionary), processed_turls_dictionary)
    else:
        message('no TURLs to verify')

    exit(0)
