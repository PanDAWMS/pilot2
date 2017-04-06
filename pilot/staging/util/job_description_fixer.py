#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Mario Lassnig, mario.lassnig@cern.ch, 2017

'''
This file is a standalone job description converter from the old description to a proposed one.
'''

import re
import json
import numbers

import logging
logger = logging.getLogger(__name__)


def camel_to_snake(name):
    '''
    Changes CamelCase to snake_case, used by python.

    :param name: name to change
    :return: name in snake_case
    '''
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def snake_to_camel(snake_str):
    '''
    Changes snake_case to firstLowCamelCase, used by server.

    :param name: name to change
    :return: name in snake_case
    '''
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


def split(val, separator=',', min_len=0, fill_last=False):
    '''
    Splits comma separated values and parses them.

    :param val:         values to split
    :param separator:   comma or whatever
    :param min_len:     minimum needed length of array, array is filled up to this value
    :param fill_last:   Flag stating the array filler, if min_value is greater then extracted array length.
                        If true, array is filled with last value, else, with Nones.
    :return: parsed array
    '''
    if val is None:
        return [None for x in range(min_len)]

    v_arr = val.split(separator)

    for i, v in enumerate(v_arr):
        v_arr[i] = parse_value(v)

    if min_len > len(v_arr):
        filler = None if not fill_last or len(v_arr) < 1 else v_arr[0]
        v_arr.extend([filler for x in range(min_len - len(v_arr))])

    return v_arr


def get_nulls(val):
    '''
    Converts every 'NULL' string to python's None.

    :param val: string or whatever
    :return: val or None if val is 'NULL'
    '''
    return val if val != 'NULL' else None


'''
forward key modifications
'''
key_fix = {
    'PandaID': 'job_id',  # it is job id, not PanDA
    'transformation': 'command',  # making it more convenient
    'jobPars': 'command_parameters',  # -.-
    'coreCount': 'cores_number',
    'prodUserID': 'user_dn',
    'prodSourceLabel': 'label',  # We don't have any other labels in there. And this is The Label, or just label
    'homepackage': 'home_package',  # lowercase, all of a sudden
    'nSent': 'throttle',  # as it's usage says
    'minRamCount': 'minimum_ram',  # reads better
    'maxDiskCount': 'maximum_input_file_size',  # what does 'count' mean? Partitions number? HDD's? Maybe number of
                                                # disks been used from the first installation of OS on that node?
    'maxCpuCount': 'maximum_cpu_usage_time',  # what does 'count' mean? Processor versions used or what?
    'attemptNr': 'attempt_number',  # bad practice to strip words API needs to be readable
}

'''
keys to be threatened as arrays
'''
arrays = []

'''
Keys, explicitly threatened as strings, even if they are all-digit or NULL
'''
key_explicit_strings = []

'''
Keys, excluded from key conversion. May be converted elsewhere.
'''
skip_keys = [
    'inFiles', 'ddmEndPointIn', 'destinationSE', 'dispatchDBlockToken', 'realDatasetsIn', 'prodDBlocks',
    'fsize',
    'checksum', 'outFiles', 'ddmEndPointOut', 'fileDestinationSE', 'dispatchDBlockTokenForOut',
    'destinationDBlockToken', 'realDatasets', 'destinationDblock', 'logGUID', 'scopeIn', 'scopeOut',
    'scopeLog',
    'GUID', 'prodDBlockToken', 'prodDBlockTokenForOut', 'dispatchDblock'
]

'''
Keys, excluded from backward key conversion. May be converted elsewhere.
'''
skip_new_keys = [
    'input_files', 'output_files'
]

'''
Backward key modifications.
Notice every all-caps abbreviations, they have to be here.
'''
key_unfix = {
    'job_id': 'PandaID',
    'user_dn': 'prodUserID',
    'task_id': 'taskID',  # all ID's are to be placed here, because snake case lacks of all-caps abbrev info
    'jobset_id': 'jobsetID',
    'job_definition_id': 'jobDefinitionID',
    'command': 'transformation',
    'command_parameters': 'jobPars',
    'cores_number': 'coreCount',
    'label': 'prodSourceLabel',
    'home_package': 'homepackage',
    'throttle': 'nSent',
    'minimum_ram': 'minRamCount',
    'maximum_input_file_size': 'maxDiskCount',
    'attempt_number': 'attemptNr',
    'status_code': 'StatusCode',
    'maximum_cpu_usage_time': 'maxCpuCount'
}


def is_float(val):
    '''
    Test floatliness of the string value.

    :param val: string or whatever
    :return: True if the value may be converted to Float
    '''
    try:
        float(val)
        return True
    except ValueError:
        return False


def is_long(s):
    '''
    Test value to be convertable to integer.

    :param s: string or whatever
    :return: True if the value may be converted to Long
    '''
    if not isinstance(s, basestring):
        try:
            long(s)
            return True
        except ValueError:
            return False

    if s[0] in ('-', '+'):
        return s[1:].isdigit()
    return s.isdigit()


def parse_value(value):
    '''
    Tries to parse value as number or None. If some of this can be done, parsed value is returned. Otherwise returns
    value unparsed.

    :param value:
    :return: mixed
    '''
    if not isinstance(value, basestring):
        return value
    if is_long(value):
        return long(value)
    if is_float(value):
        return float(value)
    return get_nulls(value)


def get_input_files(description):
    '''
    Extracts input files from the description.

    :param description:
    :return: file list
    '''
    logger.info('fixing input files in description')
    files = {}
    if description['inFiles'] and description['inFiles'] != 'NULL':
        in_files = split(description['inFiles'])
        l = len(in_files)
        ddm_endpoint = split(description.get('ddmEndPointIn'), min_len=l)
        destination_se = split(description.get('destinationSE'), min_len=l)
        dispatch_dblock = split(description.get('dispatchDblock'), min_len=l)
        dispatch_dblock_token = split(description.get('dispatchDBlockToken'), min_len=l)
        datasets = split(description.get('realDatasetsIn'), min_len=l, fill_last=True)
        dblocks = split(description.get('prodDBlocks'), min_len=l)
        dblock_tokens = split(description.get('prodDBlockToken'), min_len=l)
        size = split(description.get('fsize'), min_len=l)
        c_sum = split(description.get('checksum'), min_len=l)
        scope = split(description.get('scopeIn'), min_len=l, fill_last=True)
        guids = split(description.get('GUID'), min_len=l, fill_last=True)

        for i, f in enumerate(in_files):
            if f is not None:
                files[f] = {
                    'ddm_endpoint': ddm_endpoint[i],
                    'storage_element': destination_se[i],
                    'dispatch_dblock': dispatch_dblock[i],
                    'dispatch_dblock_token': dispatch_dblock_token[i],
                    'dataset': datasets[i],
                    'dblock': dblocks[i],
                    'dblock_token': dblock_tokens[i],
                    'size': size[i],
                    'checksum': c_sum[i],
                    'scope': scope[i],
                    'guid': guids[i]
                }
    return files


def fix_log(description, files):
    '''
    Fixes log file description in output files (changes GUID and scope).

    :param description:
    :param files: output files
    :return: fixed output files
    '''
    logger.info('modifying log-specific values in a log file description')
    if description['logFile'] and description['logFile'] != 'NULL':
        if description['logGUID'] and description['logGUID'] != 'NULL' and description['logFile'] in \
                files:
            files[description['logFile']]['guid'] = description['logGUID']
            files[description['logFile']]['scope'] = description['scopeLog']

    return files


def get_output_files(description):
    '''
    Extracts output files from the description.

    :param description:
    :return: output files
    '''
    logger.info('fixing output files in description')
    files = {}
    if description['outFiles'] and description['outFiles'] != 'NULL':
        out_files = split(description['outFiles'])
        l = len(out_files)
        ddm_endpoint = split(description.get('ddmEndPointOut'), min_len=l)
        destination_se = split(description.get('fileDestinationSE'), min_len=l)
        dblock_token = split(description.get('dispatchDBlockTokenForOut'), min_len=l)
        dblock_tokens = split(description.get('prodDBlockTokenForOut'), min_len=l)
        datasets = split(description.get('realDatasets'), min_len=l)
        dblocks = split(description.get('destinationDblock'), min_len=l)
        destination_dblock_token = split(description.get('destinationDBlockToken'), min_len=l)
        scope = split(description.get('scopeOut'), min_len=l, fill_last=True)

        for i, f in enumerate(out_files):
            if f is not None:
                files[f] = {
                    'ddm_endpoint': ddm_endpoint[i],
                    'storage_element': destination_se[i],
                    'dispatch_dblock_token': dblock_token[i],
                    'destination_dblock_token': destination_dblock_token[i],
                    'dblock_token': dblock_tokens[i],
                    'dataset': datasets[i],
                    'dblock': dblocks[i],
                    'scope': scope[i]
                }

    return fix_log(description, files)


def stringify_weird(arg):
    '''
    Converts None to 'NULL'

    :param arg:
    :return: arg or 'NULL'
    '''
    if arg is None:
        return 'NULL'
    if isinstance(arg, numbers.Number):
        return arg
    return str(arg)


def join(arr):
    '''
    Joins arrays, converting contents to strings.

    :param arr:
    :return: joined array
    '''
    return ','.join(str(stringify_weird(x)) for x in arr)


def join_input_files(unfixed, input_files):
    '''
    Diversifies the structure holding input files into old-style number of comma-separated arrays.

    :param unfixed:         oldified description structure.
    :param input_files:     input files structure
    :return:                oldified description structure with input files related fields.
    '''
    in_files = []
    ddm_endpoint = []
    destination_se = []
    dispatch_dblock = []
    dispatch_dblock_token = []
    datasets = []
    dblocks = []
    dblock_tokens = []
    size = []
    c_sum = []
    guid = []
    scope = []

    for i in input_files:
        in_files.append(i)
        ddm_endpoint.append(input_files[i]['ddm_endpoint'])
        destination_se.append(input_files[i]['storage_element'])
        dispatch_dblock.append(input_files[i]['dispatch_dblock'])
        dispatch_dblock_token.append(input_files[i]['dispatch_dblock_token'])
        datasets.append(input_files[i]['dataset'])
        dblocks.append(input_files[i]['dblock'])
        dblock_tokens.append(input_files[i]['dblock_token'])
        size.append(input_files[i]['size'])
        c_sum.append(input_files[i]['checksum'])
        scope.append(input_files[i]['scope'])  # in old description all files are in one scope, so we assume this
        guid.append(input_files[i]['guid'])

    unfixed['inFiles'] = join(in_files)
    unfixed['ddmEndPointIn'] = join(ddm_endpoint)
    unfixed['destinationSE'] = join(destination_se)
    unfixed['dispatchDblock'] = join(dispatch_dblock)
    unfixed['dispatchDBlockToken'] = join(dispatch_dblock_token)
    unfixed['realDatasetsIn'] = join(datasets)
    unfixed['prodDBlocks'] = join(dblocks)
    unfixed['prodDBlockToken'] = join(dblock_tokens)
    unfixed['fsize'] = join(size)
    unfixed['checksum'] = join(c_sum)
    unfixed['scopeIn'] = join(scope)
    unfixed['GUID'] = join(guid)

    return unfixed


def unfix_log_parameters(unfixed, log_file):
    '''
    Sets up log-related fields.

    :param unfixed:     oldified description structure.
    :param log_file:    log file structure
    :return:            oldified description structure with log fields.
    '''
    logger.info('Extracting log-specific variables')
    unfixed['logGUID'] = log_file['guid']
    unfixed['scopeLog'] = log_file['scope']
    return unfixed


def join_output_files(unfixed, output_files, log_file):
    '''
    Diversifies the structure holding output and log files into old-style number of comma-separated arrays.

    :param unfixed:         oldified description structure.
    :param output_files:    output files structure
    :param log_file:        log file name.
    :return:                oldified description structure with output and log files related fields.
    '''
    out_files = []
    ddm_endpoint = []
    destination_se = []
    dispatch_dblock_token = []
    destination_dblock_token = []
    datasets = []
    dblocks = []
    dblock_tokens = []
    scope = []

    for i in output_files:
        out_files.append(i)
        ddm_endpoint.append(output_files[i]['ddm_endpoint'])
        destination_se.append(output_files[i]['storage_element'])
        dispatch_dblock_token.append(output_files[i]['dispatch_dblock_token'])
        destination_dblock_token.append(output_files[i]['destination_dblock_token'])
        datasets.append(output_files[i]['dataset'])
        dblocks.append(output_files[i]['dblock'])
        dblock_tokens.append(output_files[i]['dblock_token'])
        if i != log_file:
            scope.append(output_files[i]['scope'])  # in old description all files are in one scope, so we assume this

    unfixed['outFiles'] = join(out_files)
    unfixed['ddmEndPointOut'] = join(ddm_endpoint)
    unfixed['fileDestinationSE'] = join(destination_se)
    unfixed['dispatchDBlockTokenForOut'] = join(dispatch_dblock_token)
    unfixed['realDatasets'] = join(datasets)
    unfixed['prodDBlockTokenForOut'] = join(dblock_tokens)
    unfixed['destinationDBlockToken'] = join(destination_dblock_token)
    unfixed['destinationDblock'] = join(dblocks)
    unfixed['scopeOut'] = join(scope)

    return unfix_log_parameters(unfixed, output_files[log_file])


def description_fixer(description, logger=None):
    '''
    Main function.

    Parses the description and changes it into more readable and usable way. For example, extracts all the files and
    makes a structure of them.

    :param description:
    :param logging.Logger(logger): logger to use. Default logger otherwise.
    :return: fixed description
    '''

    logger.info('Fixing description...')

    fixed = {}
    if isinstance(description, basestring):
        description = json.loads(description)

    logger.debug('Loaded description: ' + json.dumps(description, indent=4, sort_keys=True))

    if 'PandaID' not in description:  # already fixed
        logger.info('Description seem to be fixed already.')
        return description

    fixed['input_files'] = get_input_files(description)
    fixed['output_files'] = get_output_files(description)

    for key in description:
        value = description[key]

        if key not in skip_keys:
            if key in key_fix:
                key = key_fix[key]
            else:
                key = camel_to_snake(key)

            if key in arrays:
                fixed[key] = split(value)
            else:
                fixed[key] = parse_value(value)

    logger.debug('Fixed description: ' + json.dumps(fixed, indent=4, sort_keys=True))

    return fixed


def description_oldifier(description, logger=None):
    '''
    Main back-way parser.
    Parses the description and changes it into old one.

    :param description:
    :param logging.Logger(logger): logger to use. Default logger otherwise.
    :return: old description
    '''

    logger.info('Oldifying description...')

    unfixed = {}
    if isinstance(description, basestring):
        description = json.loads(description)

    logger.debug('Loaded description: ' + json.dumps(description, indent=4, sort_keys=True))

    if 'PandaID' in description:  # already unfixed
        logger.info('Description seem to be old enough.')
        return description

    unfixed = join_input_files(unfixed, description['input_files'])
    unfixed = join_output_files(unfixed, description['output_files'], description['log_file'])
    logger.debug(json.dumps(unfixed, indent=4, sort_keys=True))

    for key in description:
        value = description[key]

        if key not in skip_new_keys:
            if key in key_unfix:
                key = key_unfix[key]
            else:
                key = snake_to_camel(key)

            if type(value) is list:
                unfixed[key] = join(value)
            else:
                unfixed[key] = stringify_weird(value)

            if key in key_explicit_strings:
                unfixed[key] = str(unfixed[key])

    logger.debug('Fixed description: ' + json.dumps(unfixed, indent=4, sort_keys=True))

    return unfixed


def cli_parse(args):
    logger.info('loading file')
    try:
        description = json.load(args.input)
    except IOError or ValueError:
        logger.error('Could not parse file. Exiting.')
        raise

    logger.info('parsing description')
    try:
        if args.revert:
            fixed = description_oldifier(description)
        else:
            fixed = description_fixer(description)
    except Exception:
        logger.error('Could not fix description.')
        raise

    logger.info('saving file')
    try:
        json.dump(fixed, args.output, indent=4, sort_keys=True)
    except IOError or ValueError:
        logger.error('Could not save fixed description.')
        raise
