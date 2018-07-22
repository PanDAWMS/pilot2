# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Wen Guan, wen.guan@cern.ch, 2018

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

import traceback

from pilot.util import https
from pilot.util.config import config
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
    rprotocols = {}

    special_setup = {}

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
        #logger.debug('initialize StorageData from raw:\n%s' % json.dumps(data, indent=4, sort_keys=True))
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

    def get_security_key(self, secret_key, access_key):
        """
            Get security key pair from panda
            :param secret_key: secrect key name as string
            :param access_key: access key name as string
            :return: setup as a string
        """
        try:
            data = {'privateKeyName': secret_key, 'publicKeyName': access_key}

            logger.info("Getting key pair: %s" % data)
            res = https.request('{pandaserver}/server/panda/getKeyPair'.format(pandaserver=config.Pilot.pandaserver),
                                data=data)
            if res and res['StatusCode'] == 0:
                return {"publicKey": res["publicKey"], "privateKey": res["privateKey"]}
            else:
                logger.info("Got key pair returns wrong value: %s" % res)
        except Exception as ex:
            logger.error("Failed to get key pair(%s,%s): %s, %s" % (access_key, secret_key, ex, traceback.format_exc()))
        return {}

    def get_special_setup(self, protocol_id=None):
        """
            Construct special setup for ddms such as objectstore
            :return: setup as a string
        """
        logger.info("Get special setup for protocol id: %s" % protocol_id)
        if protocol_id is None or str(protocol_id) not in self.rprotocols.keys():
            return None

        if protocol_id in self.special_setup:
            return self.special_setup[protocol_id]

        if self.type in ['OS_ES', 'OS_LOGS']:
            self.special_setup[protocol_id] = None

            settings = self.rprotocols.get(str(protocol_id), {}).get('settings', {})
            access_key = settings.get('access_key', None)
            secret_key = settings.get('secret_key', None)
            is_secure = settings.get('is_secure', None)

            if access_key is None or len(access_key) == 0\
               or secret_key is None or len(secret_key) == 0\
               or is_secure is None:
                pass
            else:
                key_pair = self.get_security_key(secret_key, access_key)
                if "privateKey" not in key_pair or key_pair["privateKey"] is None:
                    logger.error("Failed to get the key pair for S3 objectstore from panda")
                else:
                    setup = "export S3_ACCESS_KEY=%s; export S3_SECRET_KEY=%s; export S3_IS_SECURE=%s;" % (key_pair["publicKey"],
                                                                                                           key_pair["privateKey"],
                                                                                                           is_secure)
                    self.special_setup[protocol_id] = setup
                    return self.special_setup[protocol_id]
        return None
