"""
The implementation of data structure to host Job definition.

The main reasons for such incapsulation are to
 - apply in one place all data validation actions (for attributes and values)
 - introduce internal information schema (names of attribues) to remove dependency
 with data structrure, formats, names from external source (PanDA)

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: February 2018
"""

import os

from .basedata import BaseData

import logging
logger = logging.getLogger(__name__)


class JobData(BaseData):
    """
        High-level object to host Job definition/settings
    """

    # ## put explicit list of all the attributes with comments for better inline-documentation by Sphinx
    # ## FIX ME LATER: use proper doc format
    # ## incomplete list of attributes .. to be extended once becomes used

    jobid = None                # Unique Job  identifier (forced to be a string)
    taskid = None               # Unique Task identifier, the task that this job belongs to (forced to be a string)

    jobparams = ""         # Job parameters defining the execution of the job
    transformation = ""    # Script execution name

    state = ""            # Current job state
    status = ""           # Current job status
    workdir = ""          # Working directoty for this job

    corecount = 1   # Number of cores as requested by the task
    platform = ""   # cmtconfig value from the task definition

    is_eventservice = False        # True for event service jobs

    _rawdata = {}  ## RAW data to keep backward compatible behavior for a while ## TO BE REMOVED once all job attributes will be covered

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['corecount'],
             str: ['jobid', 'taskid', 'jobparams', 'transformation',
                   'state', 'status', 'workdir',
                   'platform'],
             dict: [],
             bool: ['is_eventservice']
             }

    def __init__(self, data):
        """
            :param data: input dictionary of data settings
        """

        self.infosys = None  # reference to Job specific InfoService instace
        self._rawdata = data  ###  TEMPORARY CACHE -- REMOVE ME LATER once all fields moved to Job object attributes

        self.load(data)

        # DEBUG
        import pprint
        logger.debug('Initialize Job from raw:\n%s' % pprint.pformat(data))
        #logger.debug('Final parsed Job content:\n%s' % self)

    def __getitem__(self, key):
        """
            Temporary Integration function to keep dict-based access for old logic in compatible way
            TO BE REMOVED ONCE all fields will be moved to Job object attributes
        """

        if key == 'infosys':
            return self.infosys

        #if hasattr(self, key):
        #    return getattr(self, key)

        return self._rawdata[key]

    def __setitem__(self, key, val):
        """
            Temporary Integration function to keep dict-based access for old logic in compatible way
            TO BE REMOVED ONCE all fields will be moved to Job object attributes
        """

        self._rawdata[key] = val

    def __contains__(self, key):
        """
            Temporary Integration function to keep dict-based access for old logic in compatible way
            TO BE REMOVED ONCE all fields will be moved to Job object attributes
        """

        return key in self._rawdata

    def get(self, key, defval=None):
        """
            Temporary Integration function to keep dict-based access for old logic in compatible way
            TO BE REMOVED ONCE all fields will be moved to Job object attributes
        """

        return self._rawdata.get(key, defval)

    def load(self, data):
        """
            Construct and initialize data from ext source
            :param data: input dictionary of job data settings
        """

        ## the translation map of the container attributes from external data to internal schema
        ## 'internal_name':('ext_name1', 'extname2_if_any')
        ## 'internal_name2':'ext_name3'

        ## first defined ext field will be used
        ## if key is not explicitly specified then ext name will be used as is
        ## fix me later to proper internal names if need

        kmap = {
            'jobid': 'PandaID',
            'taskid': 'taskID',
            'jobparams': 'jobPars',
            'corecount': 'coreCount',
            'platform': 'cmtConfig',
            'is_eventservice': 'eventService',  ## is it coming from Job def??
        }

        self._load_data(data, kmap)

    def is_analysis(self):
        """
            Determine whether the job is an analysis user job or not.
            :return: True in case of user analysis job
        """

        is_analysis = self.transformation.startswith('https://') or self.transformation.startswith('http://')

        # apply addons checks later if need

        return is_analysis

    def is_build(self):
        """
            Determine whether the job is a build job or not.
            (i.e. check if the job only has one output file that is a lib file)
            :return: True for a build job
        """

        return False  ## TO BE IMPLEMENTED

    def clean(self):
        """
            Validate and finally clean up required data values (object properties) if need
            :return: None
        """

        pass

    ## custom function pattern to apply extra validation to the key values
    ##def clean__keyname(self, raw, value):
    ##  :param raw: raw value passed from ext source as input
    ##  :param value: preliminary cleaned and casted to proper type value
    ##
    ##    return value

    def clean__corecount(self, raw, value):
        """
            Verify and validate value for the corecount key (set to 1 if not set)
        """

        # Overwrite the corecount value with ATHENA_PROC_NUMBER if it is set
        athena_corecount = os.environ.get('ATHENA_PROC_NUMBER')
        if athena_corecount:
            try:
                value = int(athena_corecount)
            except Exception:
                logger.info("ATHENA_PROC_NUMBER is not properly set.. ignored, data=%s" % athena_corecount)

        return value if value else 1

    def clean__platform(self, raw, value):
        """
            Verify and validate value for the platform key
        """

        return value if value.lower() not in ['null', 'none'] else ''
