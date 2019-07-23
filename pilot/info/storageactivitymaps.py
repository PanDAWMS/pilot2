# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018


import logging
logger = logging.getLogger(__name__)


sched2ddm_activity_map_lan = {'es_events': 'write_lan',
                              'es_failover': 'write_lan',
                              'es_events_read': 'read_lan'}


sched2ddm_activity_map_wan = {'es_events': 'write_wan',
                              'es_failover': 'write_wan',
                              'es_events_read': 'read_wan'}


def get_ddm_activity(activity, queuedata, ddmendpoint, ddm):
    """
    Map schedconf activities to corresponding ddm activities

    :param activity: schedconf activity as string
    :param queuedata: Queue data info.
    :param ddmendpoint: ddm endpoint.
    :param ddm: ddm storage data.
    :return: ddm activity as string
    """
    if activity in sched2ddm_activity_map_lan:
        ddm_activity = sched2ddm_activity_map_lan[activity]
        if queuedata and ddm_activity in queuedata.astorages and ddmendpoint and \
                ddmendpoint in queuedata.astorages[ddm_activity] and ddm and \
                ddm_activity in ddm.arprotocols:
            pass
        else:
            ddm_activity = sched2ddm_activity_map_wan[activity]
        logger.info("Schedconf activity %s is mapped to ddm activity: %s" % (activity, ddm_activity))
        return ddm_activity
    return activity
