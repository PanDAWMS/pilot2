# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018

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
import re
import ast
import shlex
import pipes

from .basedata import BaseData
from .filespec import FileSpec

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

    transfertype = ""  # direct access
    processingtype = ""  # e.g. nightlies

    # set by the pilot (not from job definition)
    workdirsizes = []  # time ordered list of work dir sizes
    fileinfo = {}
    piloterrorcode = 0  # current pilot error code
    piloterrorcodes = []  # ordered list of stored pilot error codes
    piloterrordiag = ""  # current pilot error diagnostics
    piloterrordiags = []  # ordered list of stored pilot error diagnostics
    transexitcode = 0
    exeerrorcode = 0
    exeerrordiag = ""
    exitcode = 0
    exitmsg = ""
    state = ""  # internal pilot state; running, failed, finished, holding
    stageout = ""  # stage-out identifier, e.g. log
    metadata = {}  # payload metadata (job report)
    cpuconsumptionunit = ""
    cpuconsumptiontime = ""
    cpuconversionfactor = 1
    nevents = 0  # number of events
    neventsw = 0  # number of events written
    dbtime = None
    dbdata = None
    payload = ""  # payload name
    utilities = {}  # utility processes { <name>: [<process handle>, number of launches, command string], .. }
    pid = None  # payload pid
    pgrp = None  # process group

    # time variable used for on-the-fly cpu consumption time measurements done by job monitoring
    t0 = None  # payload startup time

    overwrite_queuedata = {}  # Custom settings extracted from job parameters (--overwriteQueueData) to be used as master values for `QueueData`
    zipmap = ""               # ZIP MAP values extracted from jobparameters
    imagename = ""            # user defined container image name extracted from job parameters

    # from job definition
    attemptnr = 0  # job attempt number
    #ddmendpointin = ""  # comma-separated list (string) of ddm endpoints for input    ## TO BE DEPRECATED: moved to FileSpec (job.indata)
    #ddmendpointout = ""  # comma-separated list (string) of ddm endpoints for output  ## TO BE DEPRECATED: moved to FileSpec (job.outdata)
    destinationdblock = ""  ## to be moved to FileSpec (job.outdata)
    datasetin = ""  ## TO BE DEPRECATED: moved to FileSpec (job.indata)
    #datasetout = ""

    infiles = ""  # comma-separated list (string) of input files  ## TO BE DEPRECATED: moved to FileSpec (use job.indata instead)
    infilesguids = ""

    indata = []   # list of `FileSpec` objects for input files (aggregated inFiles, ddmEndPointIn, scopeIn, filesizeIn, etc)
    outdata = []  # list of `FileSpec` objects for output files
    logdata = []  # list of `FileSpec` objects for log file(s)

    # home package string with additional payload release information; does not need to be added to
    # the conversion function since it's already lower case
    homepackage = ""

    jobsetid = ""  # job set id
    #logfile = ""  #  file name for log                                   ## TO BE DEPRECATED: moved to FileSpec (use job.logdata instead)
    #logguid = ""  # unique guid for log file                             ## TO BE DEPRECATED: moved to FileSpec (use job.logdata instead)
    noexecstrcnv = None  # server instruction to the pilot if it should take payload setup from job parameters
    #outfiles = ""  # comma-separated list (string) of output files       ## TO BE DEPRECATED: moved to FileSpec (job.outdata)
    #scopein = ""  # comma-separated list (string) of input file scopes   ## TO BE DEPRECATED: moved to FileSpec (job.indata)
    #scopelog = ""  # scope for log file                                  ## TO BE DEPRECATED: moved to FileSpec (use job.logdata instead)
    #scopeout = ""  # comma-separated list (string) of output file scopes ## TO BE DEPRECATED: moved to FileSpec (use job.logdata instead)
    swrelease = ""  # software release string

    # RAW data to keep backward compatible behavior for a while ## TO BE REMOVED once all job attributes will be covered
    _rawdata = {}

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['corecount', 'piloterrorcode', 'transexitcode', 'exitcode', 'cpuconversionfactor', 'exeerrorcode',
                   'attemptnr', 'nevents', 'neventsw', 'pid'],
             str: ['jobid', 'taskid', 'jobparams', 'transformation', 'destinationdblock', 'exeerrordiag'
                   'state', 'status', 'workdir', 'stageout',
                   'platform', 'piloterrordiag', 'exitmsg',
                   'infiles',         ## TO BE DEPRECATED: moved to FileSpec (job.indata)
                   #'scopein',        ## TO BE DEPRECATED: moved to FileSpec (job.indata)
                   #'outfiles', 'ddmendpointin',   ## TO BE DEPRECATED: moved to FileSpec (job.indata)
                   #'scopeout', 'ddmendpointout',    ## TO BE DEPRECATED: moved to FileSpec (job.outdata)
                   #'scopelog', 'logfile', 'logguid',            ## TO BE DEPRECATED: moved to FileSpec (job.logdata)
                   'cpuconsumptionunit', 'cpuconsumptiontime', 'homepackage', 'jobsetid', 'payload', 'processingtype',
                   'swrelease', 'zipmap', 'imagename', 'transfertype',
                   'datasetin',    ## TO BE DEPRECATED: moved to FileSpec (job.indata)
                   #'datasetout',  ## TO BE DEPRECATED: moved to FileSpec (job.outdata)
                   'infilesguids'],
             list: ['piloterrorcodes', 'piloterrordiags', 'workdirsizes'],
             dict: ['fileinfo', 'metadata', 'utilities', 'overwrite_queuedata'],
             bool: ['is_eventservice', 'noexecstrcnv']
             }

    def __init__(self, data):
        """
            :param data: input dictionary of data settings
        """

        self.infosys = None  # reference to Job specific InfoService instace
        self._rawdata = data  ###  TEMPORARY CACHE -- REMOVE ME LATER once all fields moved to Job object attributes

        self.load(data)

        self.indata = self.prepare_infiles(data)
        self.outdata, self.logdata = self.prepare_outfiles(data)

        # DEBUG
        #import pprint
        #logger.debug('Initialize Job from raw:\n%s' % pprint.pformat(data))
        logger.debug('Final parsed Job content:\n%s' % self)

    def prepare_infiles(self, data):
        """
            Construct FileSpec objects for input files from raw dict `data`
            :return: list of validated `FileSpec` objects
        """

        # form raw list data from input comma-separated values for further validataion by FileSpec
        kmap = {
            # 'internal_name': 'ext_key_structure'
            'lfn': 'inFiles',
            ##'??': 'dispatchDblock', '??define_proper_internal_name': 'dispatchDBlockToken',
            'dataset': 'realDatasetsIn', 'guid': 'GUID',
            'filesize': 'fsize', 'checksum': 'checksum', 'scope': 'scopeIn',
            ##'??define_internal_key': 'prodDBlocks',
            ##'storage_token': 'prodDBlockToken',
            'ddmendpoint': 'ddmEndPointIn',
            # 'transfertype': 'transferType',  # if transfertype was defined per file (it currently is not)
        }

        ksources = dict([k, self.clean_listdata(data.get(k, ''), list, k, [])] for k in kmap.itervalues())
        logger.debug('ksources=%s' % str(ksources))
        ret, lfns = [], set()
        for ind, lfn in enumerate(ksources.get('inFiles', [])):
            if lfn in ['', 'NULL'] or lfn in lfns:  # exclude null data and duplicates
                continue
            lfns.add(lfn)
            idat = {}
            for attrname, k in kmap.iteritems():
                idat[attrname] = ksources[k][ind] if len(ksources[k]) > ind else None
            finfo = FileSpec(type='input', **idat)
            logger.info('added file %s' % lfn)
            ret.append(finfo)

        return ret

    def prepare_outfiles(self, data):
        """
            Construct validated FileSpec objects for output and log files from raw dict `data`
            :return: (list of `FileSpec` for output, list of `FileSpec` for log)
        """

        # form raw list data from input comma-separated values for further validataion by FileSpec
        kmap = {
            # 'internal_name': 'ext_key_structure'
            'lfn': 'outFiles',
            ##'??': 'destinationDblock', '??define_proper_internal_name': 'destinationDBlockToken',
            'dataset': 'realDatasets', 'scope': 'scopeOut',
            ##'??define_internal_key':'prodDBlocks', '??':'dispatchDBlockTokenForOut',
            'ddmendpoint': 'ddmEndPointOut',
        }

        ksources = dict([k, self.clean_listdata(data.get(k, ''), list, k, [])] for k in kmap.itervalues())

        # unify scopeOut structure: add scope of log file there (better to properly fix at Panda side)
        log_lfn = data.get('logFile')
        if log_lfn:
            scope_out = []
            for lfn in ksources.get('outFiles', []):
                if lfn == log_lfn:
                    scope_out.append(data.get('scopeLog'))
                else:
                    if not ksources['scopeOut']:
                        raise Exception('Failed to extract scopeOut parameter from Job structure sent by Panda, please check input format!')
                    scope_out.append(ksources['scopeOut'].pop(0))
            ksources['scopeOut'] = scope_out

        ret_output, ret_log = [], []

        lfns = set()
        for ind, lfn in enumerate(ksources['outFiles']):
            if lfn in ['', 'NULL'] or lfn in lfns:  # exclude null data and duplicates
                continue
            lfns.add(lfn)
            idat = {}
            for attrname, k in kmap.iteritems():
                idat[attrname] = ksources[k][ind] if len(ksources[k]) > ind else None

            ftype = 'output'
            ret = ret_output
            if lfn == log_lfn:  # log file case
                ftype = 'log'
                idat['guid'] = data.get('logGUID')
                ret = ret_log

            finfo = FileSpec(type=ftype, **idat)
            ret.append(finfo)

        return ret_output, ret_log

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
            #'scopein': 'scopeIn',                        ## TO BE DEPRECATED: moved to FileSpec (job.indata)
            #'scopeout': 'scopeOut',                      ## TO BE DEPRECATED: moved to FileSpec
            #'scopelog': 'scopeLog',                      ## TO BE DEPRECATED: moved to FileSpec
            #'logfile': 'logFile',                        ## TO BE DEPRECATED: moved to FileSpec
            'infiles': 'inFiles',                        ## TO BE DEPRECATED: moved to FileSpec (job.indata)
            #'outfiles': 'outFiles',                      ## TO BE DEPRECATED: moved to FileSpec
            #'logguid': 'logGUID',                        ## TO BE DEPRECATED: moved to FileSpec
            'infilesguids': 'GUID',                      ## TO BE DEPRECATED: moved to FileSpec
            'attemptnr': 'attemptNr',
            #'ddmendpointin': 'ddmEndPointIn',            ## TO BE DEPRECATED: moved to FileSpec (job.indata)
            #'ddmendpointout': 'ddmEndPointOut',          ## TO BE DEPRECATED: moved to FileSpec
            'datasetin': 'realDatasetsIn',               ## TO BE DEPRECATED: moved to FileSpec
            #'datasetout': 'realDatasets',                ## TO BE DEPRECATED: moved to FileSpec
            'processingtype': 'processingType',
            'destinationdblock': 'destinationDblock',
            'noexecstrcnv': 'noExecStrCnv',
            'swrelease': 'swRelease',
            'jobsetid': 'jobsetID',
            'is_eventservice': 'eventService',  ## is it coming from Job def?? yes (PN)
        }

        self._load_data(data, kmap)

    def is_analysis(self):  ## if it's experiment specific logic then it could be isolated into extended JobDataATLAS class
        """
            Determine whether the job is an analysis user job or not.
            :return: True in case of user analysis job
        """

        is_analysis = self.transformation.startswith('https://') or self.transformation.startswith('http://')

        # apply addons checks later if need

        return is_analysis

    def is_build_job(self):
        """
        Check if the job is a build job.
        (i.e. check if the job has an output file that is a lib file).

        :return: boolean
        """

        for fspec in self.outdata:
            if '.lib.' in fspec.lfn and '.log.' not in fspec.lfn:
                return True

        return False

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

        # note: experiment specific

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

    def clean__jobparams(self, raw, value):
        """
        Verify and validate value for the jobparams key
        Extract value from jobparams not related to job options.
        The function will in particular extract and remove --overwriteQueueData, ZIP_MAP and --containerimage.
        It will remove the old Pilot 1 option --overwriteQueuedata which should be replaced with --overwriteQueueData.

        :param raw: (unused).
        :param value: job parameters (string).
        :return: updated job parameers (string).
        """

        ## clean job params from Pilot1 old-formatted options
        ret = re.sub(r"--overwriteQueuedata={.*?}", "", value)

        ## extract overwrite options
        options, ret = self.parse_args(ret, {'--overwriteQueueData': lambda x: ast.literal_eval(x) if x else {}}, remove=True)
        self.overwrite_queuedata = options.get('--overwriteQueueData', {})

        # extract zip map  ## TO BE FIXED? better to pass it via dedicated sub-option in jobParams from PanDA side: e.g. using --zipmap "content"
        # so that the zip_map can be handles more gracefully via parse_args

        pattern = r" \'?<ZIP_MAP>(.+)<\/ZIP_MAP>\'?"
        pattern = re.compile(pattern)

        result = re.findall(pattern, ret)
        if result:
            self.zipmap = result[0]
            # remove zip map from final jobparams
            ret = re.sub(pattern, '', ret)

        logger.debug('Extracted data from jobparams: zipmap=%s' % self.zipmap)
        logger.debug('Extracted data from jobparams: overwrite_queuedata=%s' % self.overwrite_queuedata)

        # extract and remove any present --containerimage XYZ options
        ret, imagename = self.extract_container_image(ret)
        if imagename != "":
            self.imagename = imagename

        return ret

    def extract_container_image(self, jobparams):
        """
        Extract the container image from the job parameters if present, and remove it.

        :param jobparams: job parameters (string).
        :return: updated job parameters (string), extracted image name (string).
        """

        imagename = ""

        # define regexp pattern for the full container image option
        _pattern = r'(\ \-\-containerimage\=?\s?[\S]+)'
        pattern = re.compile(_pattern)
        image_option = re.findall(pattern, jobparams)

        if image_option and image_option[0] != "":

            imagepattern = re.compile(r'(\ \-\-containerimage\=?\s?([\S]+))')
            image = re.findall(imagepattern, jobparams)
            if image and image[0] != "":
                try:
                    imagename = image[0][1]
                except Exception as e:
                    logger.warning('failed to extract image name: %s' % e)
                else:
                    logger.info("extracted image from jobparams: %s" % imagename)
            else:
                logger.warning("image could not be extract from %s" % jobparams)

            # remove the option from the job parameters
            jobparams = re.sub(_pattern, "", jobparams)
            logger.info("removed the %s option from job parameters: %s" % (image_option[0], jobparams))

        return jobparams, imagename

    @classmethod  # noqa: C901
    def parse_args(self, data, options, remove=False):
        """
            Extract option/values from string containing command line options (arguments)
            :param data: input command line arguments (raw string)
            :param options: dict of option names to be considered: (name, type), type is a cast function to be applied with result value
            :param remove: boolean, if True then exclude specified options from returned raw string of command line arguments
            :return: tuple: (dict of extracted options, raw string of final command line options)
        """

        logger.debug('Do extract options=%s from data=%s' % (options.keys(), data))

        if not options:
            return {}, data

        try:
            args = shlex.split(data)
        except ValueError, e:
            logger.error('Failed to parse input arguments from data=%s, error=%s .. skipped.' % (data, e.message))
            return {}, data

        opts, curopt, pargs = {}, None, []
        for arg in args:
            if arg.startswith('-'):
                if curopt is not None:
                    opts[curopt] = None
                    pargs.append([curopt, None])
                curopt = arg
                continue
            if curopt is None:  # no open option, ignore
                pargs.append(arg)
            else:
                opts[curopt] = arg
                pargs.append([curopt, arg])
                curopt = None
        if curopt:
            pargs.append([curopt, None])

        ret = {}
        for opt, fcast in options.iteritems():
            val = opts.get(opt)
            try:
                val = fcast(val) if callable(fcast) else val
            except Exception, e:
                logger.error('Failed to extract value for option=%s from data=%s: cast function=%s failed, exception=%s .. skipped' % (opt, val, fcast, e))
                continue
            ret[opt] = val

        ## serialize parameters back to string
        rawdata = data
        if remove:
            final_args = []
            for arg in pargs:
                if isinstance(arg, (tuple, list)):  ## parsed option
                    if arg[0] not in options:  # exclude considered options
                        if arg[1] is None:
                            arg.pop()
                        final_args.extend(arg)
                else:
                    final_args.append(arg)
            rawdata = " ".join(pipes.quote(e) for e in final_args)

        return ret, rawdata

    def add_workdir_size(self, workdir_size):
        """
        Add a measured workdir size to the workdirsizes field.
        The function will deduce any input and output file sizes from the workdir size.

        :param workdir_size: workdir size (int).
        :return:
        """

        # Convert to long if necessary
        if not isinstance(workdir_size, (int, long)):
            try:
                workdir_size = long(workdir_size)
            except Exception as e:
                logger.warning('failed to convert %s to long: %s' % (workdir_size, e))
                return

        total_size = 0L  # B

        if os.path.exists(self.workdir):
            # Find out which input and output files have been transferred and add their sizes to the total size
            # (Note: output files should also be removed from the total size since outputfilesize is added in the
            # task def)

            # Then update the file list in case additional output files were produced
            # Note: don't do this deduction since it is not known by the task definition
            # out_files, dummy, dummy = discoverAdditionalOutputFiles(outFiles, job.workdir, job.destinationDblock,
            # job.scopeOut)

            for fspec in self.indata + self.outdata:
                if fspec.type == 'input' and fspec.status != 'transferred':
                    continue
                pfn = os.path.join(self.workdir, fspec.lfn)
                if not os.path.isfile(pfn):
                    msg = "Error: pfn file=%s does not exist .. skip from wordir size calculation: %s" % pfn
                    logger.info(msg)
                total_size += os.path.getsize(pfn)

            logger.info("total size of present input+output files: %d B (workdir size: %d B)" %
                        (total_size, workdir_size))
            workdir_size -= total_size

        self.workdirsizes.append(workdir_size)

    def get_max_workdir_size(self):
        """
        Return the maximum disk space used by the payload.

        :return: workdir size (int).
        """

        maxdirsize = 0L

        if self.workdirsizes != []:
            # Get the maximum value from the list
            maxdirsize = max(self.workdirsizes)
        else:
            logger.warning("found no stored workdir sizes")

        return maxdirsize
