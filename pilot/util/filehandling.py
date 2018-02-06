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

from pilot.common.exception import PilotException, ConversionFailure, FileHandlingFailure, MKDirFailure

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
    :raises PilotException: MKDirFailure
    :return:
    """

    try:
        os.makedirs(workdir)
        os.chmod(workdir, 0770)
    except Exception as e:
        raise MKDirFailure(e)


def open_file(filename, mode):
    """
    Open and return a file pointer for the given mode.
    Note: the caller needs to close the file.

    :param filename:
    :param mode: file mode
    :raises PilotException: FileHandlingFailure
    :return: file pointer
    """

    f = None
    if os.path.exists(filename):
        try:
            f = open(filename, mode)
        except IOError as e:
            raise FileHandlingFailure(e)
    else:
        raise FileHandlingFailure("File does not exist: %s" % filename)

    return f


def convert(data):
    """
    Convert unicode data to utf-8.

    Usage examples:
    1. Dictionary:
      data = {u'Max': {u'maxRSS': 3664, u'maxSwap': 0, u'maxVMEM': 142260, u'maxPSS': 1288}, u'Avg':
             {u'avgVMEM': 94840, u'avgPSS': 850, u'avgRSS': 2430, u'avgSwap': 0}}
    convert(data)
      {'Max': {'maxRSS': 3664, 'maxSwap': 0, 'maxVMEM': 142260, 'maxPSS': 1288}, 'Avg': {'avgVMEM': 94840,
       'avgPSS': 850, 'avgRSS': 2430, 'avgSwap': 0}}
    2. String:
      data = u'hello'
    convert(data)
      'hello'
    3. List:
      data = [u'1',u'2','3']
    convert(data)
      ['1', '2', '3']

    :param data: unicode object to be converted to utf-8
    :return: converted data to utf-8
    """

    import collections
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data


def read_json(filename):
    """
    Read a dictionary with unicode to utf-8 conversion

    :param filename:
    :raises PilotException: FileHandlingFailure, ConversionFailure
    :return: json dictionary
    """

    dictionary = None
    f = open_file(filename, 'r')
    if f:
        from json import load
        try:
            dictionary = load(f)
        except PilotException as e:
            raise FileHandlingFailure(e.get_detail())
        else:
            f.close()

            # Try to convert the dictionary from unicode to utf-8
            if dictionary != {}:
                try:
                    dictionary = convert(dictionary)
                except Exception as e:
                    raise ConversionFailure(e.message)

    return dictionary


def write_json(filename, dictionary):
    """
    Write the dictionary to a JSON file.

    :param filename:
    :param dictionary:
    :raises PilotException: FileHandlingFailure
    :return: status (boolean)
    """

    status = False

    from json import dump
    try:
        fp = open(filename, "w")
    except IOError as e:
        raise FileHandlingFailure(e)
    else:
        # Write the dictionary
        try:
            dump(dictionary, fp, sort_keys=True, indent=4, separators=(',', ': '))
        except PilotException as e:
            raise FileHandlingFailure(e.get_detail())
        else:
            status = True
        fp.close()

    return status
