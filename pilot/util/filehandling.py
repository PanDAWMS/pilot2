#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os
import time

# import logging
# logger = logging.getLogger(__name__)


def get_pilot_work_dir(workdir):
    """
    Return the full path to the main PanDA Pilot work directory. Called once at the beginning of the batch job.

    :param workdir: The full path to where the main work directory should be created
    :return: The name of main work directory
    """

    jobworkdir = "PanDA_Pilot2_%d_%s" % (os.getpid(), str(int(time.time())))
    return os.path.join(workdir, jobworkdir)


def create_pilot_work_dir(workdir):
    """
    Create the main PanDA Pilot work directory.
    :param workdir: Full path to the directory to be created
    :return:
    """

    try:
        os.makedirs(workdir)
        os.chmod(workdir, 0770)
    except Exception:  # , e:
        # logger.error('could not create main work directory: %s' % e)
        # throw PilotException here
        pass


def open_file(filename, mode):
    """
    Open and return a file pointer for the given mode.
    Note: the caller needs to close the file.

    :param filename:
    :param mode: file mode
    :return: file pointer
    """

    f = None
    if os.path.exists(filename):
        try:
            f = open(filename, mode)
        except IOError, e:
            pass
            # raise exception   tolog("!!WARNING!!2997!! Caught exception: %s" % (e))
    else:
        pass
        # raise exception   tolog("!!WARNING!!2998!! File does not exist: %s" % (filename))

    return f


def get_json_dictionary(filename):
    """
    Read a dictionary with unicode to utf-8 conversion

    :param filename:
    :return: json dictionary
    """

    dictionary = None
    f = open_file(filename, 'r')
    if f:
        from json import load
        try:
            dictionary = load(f)
        except Exception, e:
            pass
            # raise exception   tolog("!!WARNING!!2222!! Failed to load json dictionary: %s" % (e))
        else:
            f.close()

            # Try to convert the dictionary from unicode to utf-8
            if dictionary != {}:
                try:
                    dictionary = convert(dictionary)
                except Exception, e:
                    pass
                    # raise exception   tolog("!!WARNING!!2996!! Failed to convert dictionary from unicode to utf-8: %s, %s" % (dictionary, e))
            else:
                pass
                # raise exception   tolog("!!WARNING!!2995!! Load function returned empty JSON dictionary: %s" % (filename))

    return dictionary


def write_json(filename, dictionary):
    """
    Write the dictionary to a JSON file.

    :param filename:
    :param dictionary:
    :return: status (boolean)
    """

    status = False

    from json import dump
    try:
        fp = open(filename, "w")
    except Exception, e:
        pass
        # raise exception  tolog("!!WARNING!!2323!! Failed to open file %s: %s" % (filename, e))
    else:
        # Write the dictionary
        try:
            dump(dictionary, fp, sort_keys=True, indent=4, separators=(',', ': '))
        except Exception, e:
            pass
            # raise exception   tolog("!!WARNING!!2324!! Failed to write dictionary to file %s: %s" % (filename, e))
        else:
            status = True
        fp.close()

    return status
