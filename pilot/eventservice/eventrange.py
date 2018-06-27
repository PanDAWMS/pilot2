#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018

import json

from pilot.util import https
from pilot.util.auxiliary import get_logger
from pilot.util.config import config

import logging
logger = logging.getLogger(__name__)


def download_event_ranges(job, num_ranges=None):
    """
    Download event ranges

    :param job:
    :param num_ranges:

    :return: List of event ranges.
    """

    log = get_logger(job.jobid)

    try:
        if not num_ranges:
            # ToBeFix num_ranges with corecount
            num_ranges = 1

        data = {'pandaID': job.jobid,
                'jobsetID': job.jobsetid,
                'taskID': job.taskid,
                'nRanges': num_ranges}

        log.info("Downloading new event ranges: %s" % data)
        res = https.request('{pandaserver}/server/panda/getEventRanges'.format(pandaserver=config.Pilot.pandaserver),
                            data=data)
        log.info("Downloaded event ranges: %s" % res)
        if res['StatusCode'] == 0 or str(res['StatusCode']) == '0':
            return res['eventRanges']

        return []
    except Exception as e:
        log.error("Failed to download event ranges: %s" % (e.get_detail()))
    return None


def update_event_ranges(job, event_ranges, version=1):
    """
    Update an event range on the Event Server

    :param event_ranges:
    """
    log = get_logger(job.jobid)

    log.info("Updating event ranges: %s" % event_ranges)

    try:
        if version:
            data = {'eventRanges': json.dumps(event_ranges), 'version': 1}
        else:
            data = {'eventRanges': json.dumps(event_ranges)}

        log.info("Updating event ranges: %s" % data)
        res = https.request('{pandaserver}/server/panda/updateEventRanges'.format(pandaserver=config.Pilot.pandaserver),
                            data=data)

        log.info("Updated event ranges status: %s" % res)
    except Exception as e:
        log.error("Failed to update event ranges: %s" % (e.get_detail()))
