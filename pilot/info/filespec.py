# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018-2019

"""
The implementation of data structure to host File related data description.

The main reasons for such incapsulation are to
 - apply in one place all data validation actions (for attributes and values)
 - introduce internal information schema (names of attribues) to remove direct dependency to ext storage/structures

:author: Alexey Anisenkov
:date: April 2018
"""

from .basedata import BaseData

import logging
logger = logging.getLogger(__name__)


class FileSpec(BaseData):
    """
        High-level object to host File Specification (meta data like lfn, checksum, replica details, etc.)
    """

    ## put explicit list of all the attributes with comments for better inline-documentation by sphinx
    ## FIX ME LATER: use proper doc format

    ## incomplete list of attributes .. to be extended once becomes used

    lfn = ""
    guid = ""
    filesize = 0
    checksum = {}    # file checksum values, allowed keys=['adler32', 'md5'], e.g. `fspec.checksum.get('adler32')`
    scope = ""       # file scope
    dataset = ""
    ddmendpoint = ""    ## DDMEndpoint name (input or output depending on FileSpec.filetype)
    accessmode = ""  # preferred access mode
    allow_lan = True
    allow_wan = False
    direct_access_lan = False
    direct_access_wan = False
    storage_token = ""  # prodDBlockToken = ""      # moved from Pilot1: suggest proper internal name (storage token?)
    ## dispatchDblock =  ""       # moved from Pilot1: is it needed? suggest proper internal name?
    ## dispatchDBlockToken = ""   # moved from Pilot1: is it needed? suggest proper internal name?
    ## prodDBlock = ""           # moved from Pilot1: is it needed? suggest proper internal name?

    ## local keys
    filetype = ''      # type of File: input, output of log
    replicas = None    # list of resolved input replicas
    protocols = None   # list of preferred protocols for requested activity
    surl = ''          # source url
    turl = ''          # transfer url
    domain = ""        # domain of resolved replica
    mtime = 0          # file modification time
    status = None      # file transfer status value
    status_code = 0    # file transfer status code
    inputddms = []     # list of DDMEndpoint names which will be considered by default (if set) as allowed local (LAN) storage for input replicas
    workdir = None     # used to declare file-specific work dir (location of given local file when it's used for transfer by copytool)
    protocol_id = None  # id of the protocol to be used to construct turl
    is_tar = False     # whether it's a tar file or not
    ddm_activity = None  # DDM activity names (e.g. [read_lan, read_wan]) which should be used to resolve appropriate protocols from StorageData.arprotocols

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['filesize', 'mtime', 'status_code'],
             str: ['lfn', 'guid', 'checksum', 'scope', 'dataset', 'ddmendpoint',
                   'filetype', 'surl', 'turl', 'domain', 'status', 'workdir', 'accessmode', 'storage_token'],
             list: ['replicas', 'inputddms', 'ddm_activity'],
             bool: ['allow_lan', 'allow_wan', 'direct_access_lan', 'direct_access_wan']
             }

    def __init__(self, filetype='input', **data):  ## FileSpec can be split into FileSpecInput + FileSpecOuput classes in case of significant logic changes
        """
            :param kwargs: input dictionary of object description
            :param type: type of File: either input, output or log
        """

        self.filetype = filetype
        self.load(data)

    def load(self, data):
        """
            Construct and initialize data from ext source for Input `FileSpec`
            :param data: input dictionary of object description
        """

        # the translation map of the key attributes from external data to internal schema
        # if key is not explicitly specified then ext name will be used as is

        kmap = {
            # 'internal_name2': 'ext_name3'
        }

        self._load_data(data, kmap)

    ## custom function pattern to apply extra validation to the key values
    ##def clean__keyname(self, raw, value):
    ##  :param raw: raw value passed from ext source as input
    ##  :param value: preliminary cleaned and casted to proper type value
    ##
    ##    return value

    def clean__checksum(self, raw, value):
        """
            Validate value for the checksum key
            Expected raw format is 'ad:value' or 'md:value'
        """

        if isinstance(value, dict):
            return value

        cmap = {'ad': 'adler32', 'md': 'md5'}

        ctype, checksum = 'adler32', value
        cc = value.split(':')
        if len(cc) == 2:
            ctype, checksum = cc
            ctype = cmap.get(ctype) or 'adler32'

        return {ctype: checksum}

    def clean(self):
        """
            Validate and finally clean up required data values (required object properties) if need
            Executed once all fields have already passed field-specific validation checks
            Could be customized by child object
            :return: None
        """
        if self.lfn.startswith("zip://"):
            self.lfn = self.lfn.replace("zip://", "")
            self.is_tar = True

    def is_directaccess(self, ensure_replica=True, allowed_replica_schemas=None):
        """
            Check if given (input) file can be used for direct access mode by Job transformation script
            :param ensure_replica: boolean, if True then check by allowed schemas of file replica turl will be considered as well
            :return: boolean
        """

        # check by filename pattern
        filename = self.lfn.lower()

        is_rootfile = True
        exclude_pattern = ['.tar.gz', '.lib.tgz', '.raw.']
        for e in exclude_pattern:
            if e in filename:
                is_rootfile = False
                break

        if not is_rootfile:
            return False

        is_directaccess = False  ## default value
        if self.accessmode == 'direct':
            is_directaccess = True
        elif self.accessmode == 'copy':
            is_directaccess = False

        if ensure_replica:

            allowed_replica_schemas = allowed_replica_schemas or ['root', 'dcache', 'dcap', 'file', 'https']
            if not self.turl or not any([self.turl.startswith('%s://' % e) for e in allowed_replica_schemas]):
                is_directaccess = False

        return is_directaccess

    def get_storage_id_and_path_convention(self):
        """
        Parse storage_token to get storage_id and path_convention.
         :param storage_token: string, expected format is '<normal storage token as string>', '<storage_id as int>', <storage_id as int/path_convention as int>
        :returns: storage_id, path_convention
        """
        storage_id = None
        path_convention = None
        try:
            if self.storage_token:
                if self.storage_token.count('/') == 1:
                    storage_id, path_convention = self.storage_token.split('/')
                    storage_id = int(storage_id)
                    path_convention = int(path_convention)
                elif self.storage_token.isdigit():
                    storage_id = int(self.storage_token)
        except Exception as ex:
            logger.warning("Failed to parse storage_token(%s): %s" % (self.storage_token, ex))
        logger.info('storage_id: %s, path_convention: %s' % (storage_id, path_convention))
        return storage_id, path_convention
