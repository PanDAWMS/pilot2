#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

# Note: stage-out not implemented; replica resolution donein copy_in - but is now done at an earlier stage

import re

from pilot.copytool.common import merge_destinations
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)

require_replicas = True  ## indicate if given copytool requires input replicas to be resolved


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER
    #for f in files:
    #    if not all(key in f for key in ('scope', 'name', 'destination')):
    #        return False
    #return True


def is_valid_for_copy_out(files):
    return False  # NOT IMPLEMENTED YET
    #for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    #return True


def copy_in(files):
    """
    Tries to download the given files using xrdcp directly.

    :param files: Files to download

    :raises Exception
    """
    destinations = merge_destinations(files)
    if len(destinations) == 0:
        raise Exception('no lfn with existing destination path given!')

    lfns = set()
    for dst in destinations:
        lfns.update(destinations[dst]['lfns'])

    # this part should no longer be necessary - replicas are resolved at an earlier stage

    executable = ['/usr/bin/env',
                  'rucio', '-R', 'list-file-replicas',
                  '--protocols', 'root']
    executable.extend(lfns)

    logger.info('querying file replicas from rucio...')

    exit_code, stdout, stderr = execute(executable)

    if exit_code != 0:
        raise Exception('could not query file replicas from rucio!')

    # | scope | name | size | hash | RSE: pfn |\n
    pattern = ur'^\s*\|\s*(\S*)\s*\|\s*(\S*)\s*\|\s*[0-9]*\s*\|\s*[0-9a-zA-Z]{8}\s*\|\s*(\S*):\s*(root://\S*).*$'
    regex = re.compile(pattern, re.MULTILINE)
    lfns_with_pfns = {}
    for match in regex.finditer(stdout):
        # [1] = scope, [2] = name, [3] = rse, [4] = pfn
        grps = match.groups()

        if len(grps) != 4:
            logger.warning('regex returned unexpected amount of matches! ignoring match...')
            continue
        if None in grps:
            logger.warning('match contained None! ignoring match...')
            continue

        lfn = '%s:%s' % (grps[0], grps[1])
        lfns_with_pfns.setdefault(lfn, []).append(grps[3])

    for dst in destinations:
        executable = ['/usr/bin/env',
                      'xrdcp', '-f']
        for lfn in destinations[dst]['lfns']:
            if lfn not in lfns_with_pfns:
                raise Exception('the given LFNs %s were not returned by Rucio!' % lfn)
            executable.append(lfns_with_pfns[lfn][0])

        executable.append(dst)

        exit_code, stdout, stderr = execute(executable)

        stats = {}
        if exit_code == 0:
            stats['status'] = 'done'
            stats['errno'] = 0
            stats['errmsg'] = 'file successfully downloaded.'
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
