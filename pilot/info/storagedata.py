# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018

"""
The implementation of data structure to host storage data description.

The main reasons for such incapsulation are to
 - apply in one place all data validation actions (for attributes and values)
 - introduce internal information schema (names of attribues) to remove direct dependency
 with data structrure, formats, names from external sources (e.g. AGIS/CRIC)

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

from .basedata import BaseData

import logging
logger = logging.getLogger(__name__)


class StorageData(BaseData):
    """
        High-level object to host Storage details (available protocols, etc.)
    """

    ## put explicit list of all the attributes with comments for better inline-documentation by sphinx
    ## FIX ME LATER: use proper doc format

    ## incomplete list of attributes .. to be extended once becomes used

    pk = 0        # unique identification number
    name = ""     # DDMEndpoint name
    type = ""     # type of Storage
    token = ""    # space token descriptor

    is_deterministic = None

    state = None
    site = None   # ATLAS Site name

    arprotocols = {}

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['pk'],
             str: ['name', 'state', 'site', 'type', 'token'],
             dict: ['copytools', 'acopytools', 'astorages', 'arprotocols', 'rprotocols'],
             bool: ['is_deterministic']
             }

    def __init__(self, data):
        """
            :param data: input dictionary of storage description by DDMEndpoint name as key
        """

        self.load(data)

        # DEBUG
        #import pprint
        #logger.debug('initialize StorageData from raw:\n%s' % pprint.pformat(data))
        #logger.debug('Final parsed StorageData content:\n%s' % self)

    def load(self, data):
        """
            Construct and initialize data from ext source
            :param data: input dictionary of storage description by DDMEndpoint name as key
        """

        # the translation map of the queue data attributes from external data to internal schema
        # first defined ext field name will be used
        # if key is not explicitly specified then ext name will be used as is
        ## fix me later to proper internal names if need

        kmap = {
            # 'internal_name': ('ext_name1', 'extname2_if_any')
            # 'internal_name2': 'ext_name3'
            'pk': 'id',
        }

        self._load_data(data, kmap)

    ## custom function pattern to apply extra validation to the key values
    ##def clean__keyname(self, raw, value):
    ##  :param raw: raw value passed from ext source as input
    ##  :param value: preliminary cleaned and casted to proper type value
    ##
    ##    return value
