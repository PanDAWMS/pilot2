"""
The implementation of data structure to host queuedata settings.

The main reasons for such incapsulation are to
 - apply in one place all data validation actions (for attributes and values)
 - introduce internal information schema (names of attribues) to remove dependency
 with data structrure, formats, names from external sources (e.g. AGIS/CRIC)

This module should be standalone as much as possible and even does not depend
on the configuration settings
(for that purposed `PilotConfigProvider` can be user to customize data)

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

from .basedata import BaseData

import logging
logger = logging.getLogger(__name__)

class QueueData(BaseData):
    """
        High-level object to host all queuedata settings associated to given PandaQueue
    """

    ## put explicit list of all the attributes with comments for better inline-documentation by sphinx
    ## FIX ME LATER: use proper doc format

    ## incomplete list of attributes .. to be extended once becomes used

    name = ""       # Name of Panda Queue
    appdir = ""     #
    catchall = ""   #

    cmtconfig = ""
    container_options = ""
    container_type = ""

    copytools = None
    acopytools = None
    astorages = None
    aprotocols = None

    state = None
    site = None   # ATLAS Site name

    direct_access_lan = False
    direct_access_lan = False

    maxwdir = 0   # in MB

    timefloor = 0 # The maximum time during which the pilot is allowed to start a new job, in seconds

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['timefloor'],
             str: ['name', 'appdir', 'catchall', 'cmtconfig', 'container_options', 'container_type',
                   'state', 'site'],
             dict: ['copytools', 'acopytools', 'astorages', 'aprotocols'],
             bool: ['direct_access_lan', 'direct_access_wan']
            }


    def __init__(self, data):
        """
            :param data: input dictionary of queue data settings
        """

        self.load(data)

        # DEBUG
        import pprint
        logger.debug('initialize QueueData from raw:\n%s' % pprint.pformat(data))
        logger.debug('Final parsed QueueData content:\n%s' % self)


    def load(self, data):
        """
            Construct and initialize data from ext source
            :param data: input dictionary of queue data settings
        """

        # the translation map of the queue data attributes from external data to internal schema
        # 'internal_name':('ext_name1', 'extname2_if_any')
        # 'internal_name2':'ext_name3'

        # first defined ext field will be used
        # if key is not explicitly specified then ext name will be used as is
        ## fix me later to proper internal names if need

        kmap = {
            'name': 'nickname',
            'site': ('atlas_site', 'gstat'),
            }

        self._load_data(data, kmap)


    ## custom function pattern to apply extra validation to the key values
    ##def clean__keyname(self, raw, value):
    ##  :param raw: raw value passed from ext source as input
    ##  :param value: preliminary cleaned and casted to proper type value
    ##
    ##    return value

    def clean__timefloor(self, raw, value):
        """
            Verify and validate value for the timefloor key (convert to seconds)
        """

        return value * 60
