# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018

## THIS FILE IS DEPRECATED AND CAN BE REMOVED LATER (anisyonk)

import logging
logger = logging.getLogger(__name__)


sched2ddm_activity_map = {'es_events': 'pw',
                          'es_failover': 'pw',
                          'es_events_read': 'pr'}


def get_ddm_activity(activity):
    """
    Map schedconf activities to corresponding ddm activities

    :param activity: schedconf activity as string
    :return: ddm activity as string
    """
    if activity in sched2ddm_activity_map:
        ddm_activity = sched2ddm_activity_map[activity]
        logger.info("Schedconf activity %s is mapped to ddm activity: %s" % (activity, ddm_activity))
        return ddm_activity
    return activity
