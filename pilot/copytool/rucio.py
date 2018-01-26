#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017

import os

from pilot.copytool.common import merge_destinations
from pilot.util.container import execute


def copy_in(files):
    """
    Tries to download the given files using rucio.

    :param files: Files to download

    :raises Exception
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    destinations = merge_destinations(files)

    if len(destinations) == 0:
        raise Exception('No lfn with existing destination path given!')

    for dst in destinations:
        executable = ['/usr/bin/env',
                      'rucio', 'download',
                      '--no-subdir',
                      '--dir', dst]
        executable.extend(destinations[dst]['lfns'])

        # process = subprocess.Popen(executable,
        #                            bufsize=-1,
        #                            stdout=subprocess.PIPE,
        #                            stderr=subprocess.PIPE)
        # stdout, stderr = process.communicate()
        # exit_code = process.poll()

        exit_code, stdout, stderr = execute(executable)

        stats = {}
        if exit_code == 0:
            stats['status'] = 'done'
            stats['errno'] = 0
            stats['errmsg'] = 'File successfully downloaded.'
        else:
            stats['status'] = 'failed'
            stats['errno'] = 3
            try:
                # the Details: string is set in rucio: lib/rucio/common/exception.py in __str__()
                stats['errmsg'] = [detail for detail in stderr.split('\n') if detail.startswith('Details:')][0][9:-1]
            except Exception as e:
                stats['errmsg'] = 'Could not find rucio error message details - please check stderr directly: %s' % \
                                  str(e)
        for f in destinations[dst]['files']:
            f.update(stats)
    return files


def copy_out(files):
    """
    Tries to upload the given files using rucio

    :param files Files to download

    :raises Exception
    """
    raise NotImplementedError()
