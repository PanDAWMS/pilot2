#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017

import os
import re
import subprocess
import time

import logging
logger = logging.getLogger(__name__)

def copy_rucio(site, files):
    """
    Separate dummy implementation for automatic stage-in outside of pilot workflows.
    Should be merged with regular stage-in functionality later, but we need to have
    some operational experience with it first.
    Many things to improve:
     - separate file error handling in the merged case
     - auto-merging of files with same destination into single copytool call
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    # quickly remove non-existing destinations
    for f in files:
        if not os.path.exists(f['destination']):
            f['status'] = 'failed'
            f['errmsg'] = 'Destination directory does not exist: %s' % f['destination']
            f['errno'] = 1
        else:
            f['status'] = 'transferring'
            f['errmsg'] = 'File not yet successfully downloaded.'
            f['errno'] = 2

    for f in files:
        if f['errno'] == 1:
            continue
        
        did_str = '%s:%s' % (f['scope'], f['name'])
        executable = ['/usr/bin/env',
                      'rucio', 'download',
                      '--no-subdir',
                      '--dir', f['destination'],
                      did_str]

        process = subprocess.Popen(executable,
                                   bufsize=-1,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        f['errno'] = 2
        while True:
            time.sleep(0.1)
            exit_code = process.poll()
            if exit_code is not None:
                stdout, stderr = process.communicate()
                if exit_code == 0:
                    f['status'] = 'done'
                    f['errno'] = 0
                    f['errmsg'] = 'File successfully downloaded.'
                else:
                    f['status'] = 'failed'
                    f['errno'] = 3
                    try:
                        # the Details: string is set in rucio: lib/rucio/common/exception.py in __str__()
                        f['errmsg'] = [detail for detail in stderr.split('\n') if detail.startswith('Details:')][0][9:-1]
                    except Exception as e:
                        f['errmsg'] = 'Could not find rucio error message details - please check stderr directly: %s' % str(e)
                break
            else:
                continue

    return files

def copy_xrdcp(site, files):
    """ Provides access to files stored inside connected the RSE.

        :param pfn Physical file name of requested file
        :param dest Name and path of the files when stored at the client

        :raises DestinationNotAccessible, ServiceUnavailable, SourceNotFound
    """
    executable = ['/usr/bin/env',
                  'rucio', '-R', 'list-file-replicas',
                  '--protocols', 'root']
    for f in files:
        executable.append('%s:%s' % (f['scope'], f['name']))

    logger.info('Querying file replicas from rucio...')
    process = subprocess.Popen(executable,
                               bufsize=-1,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    exit_code = process.poll()

    if exit_code != 0:
        raise Exception('Could not query file replicas from rucio!')

    pattern = ur'^\s*\|\s*(\S*)\s*\|\s*(\S*)\s*\|\s*[0-9]*\s*\|\s*[0-9a-zA-Z]{8}\s*\|\s*(\S*):\s*(root://\S*).*$'
    regex = re.compile(pattern, re.MULTILINE)
    lfn_with_rsepfn = {}
    for match in regex.finditer(stdout):
        grps = match.groups()

        if len(grps) != 4:
            logger.warning('Regex returned unexpected amount of matches! Ignoring match...')
            continue
        if None in grps:
            logger.warning('Match contained None! Ignoring match...')
            continue

        did_str = '%s:%s' % (grps[0], grps[1])
        lfn_with_rsepfn.setdefault(did_str, {})[grps[2]] = grps[3]

    if len(lfn_with_rsepfn) == 0:
        raise Exception('Could not extract replicas from rucio output')

    try:
        cmd = 'xrdcp -f %s %s' % (pfn, dest)
        status, out, err = execute(cmd)
        if status == 54:
            raise exception.SourceNotFound()
        elif status != 0:
            raise exception.RucioException(err)
    except Exception as e:
        raise exception.ServiceUnavailable(e)
