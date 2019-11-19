# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

"""
Pilot Information component

A set of low-level information providers to aggregate, prioritize (overwrite),
hide dependency to external storages and expose (queue, site, storage, etc) details
in a unified structured way to all Pilot modules by providing high-level API

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""


from .infoservice import InfoService
from .jobinfo import JobInfoProvider  # noqa
from .jobdata import JobData          # noqa
from .filespec import FileSpec        # noqa

#from .queuedata import QueueData

from pilot.common.exception import PilotException

import collections

import logging
logger = logging.getLogger(__name__)


def set_info(args):   ## should be DEPRECATED: use `infosys.init(queuename)`
    """
    Set up all necessary site information for given PandaQueue name.
    Resolve everything from the specified queue name (passed via `args.queue`)
    and fill extra lookup structure (Populate `args.info`).

    raise PilotException in case of errors.

    :param args: input (shared) agruments
    :return: None
    """

    # ## initialize info service
    infosys.init(args.queue)

    args.info = collections.namedtuple('info', ['queue', 'infoservice',
                                                # queuedata,
                                                'site', 'storages',
                                                # 'site_info',
                                                'storages_info'])
    args.info.queue = args.queue
    args.info.infoservice = infosys  # ## THIS is actually for tests and redundant - the pilot.info.infosys should be used
    # args.infoservice = infosys  # ??

    # check if queue is ACTIVE
    if infosys.queuedata.state != 'ACTIVE':
        logger.critical('specified queue is NOT ACTIVE: %s -- aborting' % infosys.queuedata.name)
        raise PilotException("Panda Queue is NOT ACTIVE")

    # do we need explicit varible declaration (queuedata)?
    # same as args.location.infoservice.queuedata
    #args.location.queuedata = infosys.queuedata

    # do we need explicit varible declaration (Experiment site name)?
    # same as args.location.infoservice.queuedata.site
    #args.location.site = infosys.queuedata.site

    # do we need explicit varible declaration (storages_info)?
    # same as args.location.infoservice.storages_info
    #args.location.storages_info = infosys.storages_info

    # find all enabled storages at site
    try:
        args.info.storages = [ddm for ddm, dat in infosys.storages_info.iteritems() if dat.site == infosys.queuedata.site]  # Python 2
    except Exception:
        args.info.storages = [ddm for ddm, dat in list(infosys.storages_info.items()) if dat.site == infosys.queuedata.site]  # Python 3

    #args.info.sites_info = infosys.sites_info

    logger.info('queue: %s' % args.info.queue)
    #logger.info('site: %s' % args.info.site)
    #logger.info('storages: %s' % args.info.storages)
    #logger.info('queuedata: %s' % args.info.infoservice.queuedata)


# global InfoService Instance without Job specific settings applied (singleton shared object)
# normally we should create such instance for each job to properly consider overwrites coming from JonInfoProvider
# Initialization required to access the data
infosys = InfoService()
