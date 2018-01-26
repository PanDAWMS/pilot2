#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import re

from pilot.copytool.common import merge_destinations
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def copy_in(files):
    """
    Tries to download the given files using xrdcp directly.

    :param files: Files to download

    :raises Exception
    """
    destinations = merge_destinations(files)
    if len(destinations) == 0:
        raise Exception('No lfn with existing destination path given!')

    lfns = set()
    for dst in destinations:
        lfns.update(destinations[dst]['lfns'])

    executable = ['/usr/bin/env',
                  'rucio', '-R', 'list-file-replicas',
                  '--protocols', 'root']
    executable.extend(lfns)

    logger.info('Querying file replicas from rucio...')

    # process = subprocess.Popen(executable,
    #                            bufsize=-1,
    #                            stdout=subprocess.PIPE,
    #                            stderr=subprocess.PIPE)
    # stdout, stderr = process.communicate()
    # exit_code = process.poll()

    exit_code, stdout, stderr = execute(executable)

    if exit_code != 0:
        raise Exception('Could not query file replicas from rucio!')

    # | scope | name | size | hash | RSE: pfn |\n
    pattern = ur'^\s*\|\s*(\S*)\s*\|\s*(\S*)\s*\|\s*[0-9]*\s*\|\s*[0-9a-zA-Z]{8}\s*\|\s*(\S*):\s*(root://\S*).*$'
    regex = re.compile(pattern, re.MULTILINE)
    lfns_with_pfns = {}
    for match in regex.finditer(stdout):
        # [1] = scope, [2] = name, [3] = rse, [4] = pfn
        grps = match.groups()

        if len(grps) != 4:
            logger.warning('Regex returned unexpected amount of matches! Ignoring match...')
            continue
        if None in grps:
            logger.warning('Match contained None! Ignoring match...')
            continue

        lfn = '%s:%s' % (grps[0], grps[1])
        lfns_with_pfns.setdefault(lfn, []).append(grps[3])

    for dst in destinations:
        executable = ['/usr/bin/env',
                      'xrdcp', '-f']
        for lfn in destinations[dst]['lfns']:
            if lfn not in lfns_with_pfns:
                raise Exception('The given LFNs %s were not returned by Rucio!' % lfn)
            executable.append(lfns_with_pfns[lfn][0])

        executable.append(dst)

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
            stats['errmsg'] = stderr

        for f in destinations[dst]['files']:
            f.update(stats)
    return files


def copy_out(files):
    """
    Tries to upload the given files using xrdcp directly.

    :param files Files to download

    :raises Exception
    """
    raise NotImplementedError()
