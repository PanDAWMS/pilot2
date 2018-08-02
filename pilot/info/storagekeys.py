# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018


import traceback

from pilot.util import https
from pilot.util.config import config

import logging
logger = logging.getLogger(__name__)


class StorageKeys(object):
    """
    Singleton class to manage key pairs for storages such as objectstores.
    """

    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance

    def __init__(self):
        """
        Init empty storage_keys dictionary.
        The structor of storage_keys: {'ddmenpoint': {'protocol_id': {'special_setup': <>}}}
        """
        self.storage_keys = {}

    def get_security_key(self, secret_key, access_key):
        """
        Get security key pair from panda
        :param secret_key: secrect key name as string
        :param access_key: access key name as string
        :return: securet keypairs from panda.
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

    def get_special_setup(self, ddmendpoint, protocol_id):
        """
        Construct special setup for ddms such as objectstore
        :param ddmendpoint: storage data
        :param protocol_id: protocol id.
        :return: setup as a string
        """

        logger.info("Get special setup for ddmendpoint(%s) protocol id(%s)" % (ddmendpoint.name, protocol_id))
        if protocol_id is None or str(protocol_id) not in ddmendpoint.rprotocols.keys():
            return None

        if ddmendpoint.name in self.storage_keys and protocol_id in self.storage_keys[ddmendpoint.name]:
            return self.storage_keys[ddmendpoint.name][protocol_id]['special_setup']

        if ddmendpoint.type in ['OS_ES', 'OS_LOGS']:
            if ddmendpoint.name not in self.storage_keys:
                self.storage_keys[ddmendpoint.name] = {}
            if protocol_id not in self.storage_keys[ddmendpoint.name]:
                self.storage_keys[ddmendpoint.name][protocol_id] = {}
            self.storage_keys[ddmendpoint.name][protocol_id]['special_setup'] = None

            settings = ddmendpoint.rprotocols.get(str(protocol_id), {}).get('settings', {})
            access_key = settings.get('access_key', None)
            secret_key = settings.get('secret_key', None)
            is_secure = settings.get('is_secure', None)

            # make sure all things are correctly defined in AGIS.
            # If one of them is not defined correctly, will not setup this part. Then rucio client can try to use signed url.
            # This part is preferred because signed url is not efficient.
            if access_key and secret_key and is_secure:
                key_pair = self.get_security_key(secret_key, access_key)
                if "privateKey" not in key_pair or key_pair["privateKey"] is None:
                    logger.error("Failed to get the key pair for S3 objectstore from panda")
                else:
                    setup = "export S3_ACCESS_KEY=%s; export S3_SECRET_KEY=%s; export S3_IS_SECURE=%s;" % (key_pair["publicKey"],
                                                                                                           key_pair["privateKey"],
                                                                                                           is_secure)
                    self.storage_keys[ddmendpoint.name][protocol_id]['special_setup'] = setup
                    logger.info("Return key pair with public key: %s" % key_pair["publicKey"])
                    return self.storage_keys[ddmendpoint.name][protocol_id]['special_setup']
        return None


storage_keys = StorageKeys()
