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


import logging
logger = logging.getLogger(__name__)


class QueueData(object):
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

    direct_access_lan = False
    direct_access_lan = False

    timefloor = 0 # The maximum time during which the pilot is allowed to start a new job, in seconds

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['timefloor'],
             str: ['name', 'appdir', 'catchall', 'cmtconfig', 'container_options', 'container_type'],
             dict: ['copytools', 'acopytools', 'astorages', 'aprotocols'],
             bool: ['direct_access_lan', 'direct_access_wan']
            }


    def __init__(self, data):
        """
            :param data: input dictionary of queue data settings
        """

        self.init(data)

        import pprint
        logger.debug('initialize QueueData from raw:\n%s' % pprint.pformat(data))
        logger.info('Final parsed QueueData content:\n%s' % self)


    def init(self, data):
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
            'name':('nickname',),
            }

        validators = {int: self.clean_numeric,
                      str: self.clean_string,
                      bool: self.clean_boolean,
                      dict: self.clean_dictdata,

                      None: self.clean_string, # default validator
                    }

        for ktype, knames in self._keys.iteritems():

            for kname in knames:
                raw, value = None, None

                ext_names = kmap.get(kname) or kname
                if isinstance(ext_names, basestring):
                    ext_names = [ext_names]
                for name in ext_names:
                    raw = data.get(name)
                    if raw is not None:
                        break

                ## cast to required type and apply default validation
                hvalidator = validators.get(ktype, validators.get(None))
                if callable(hvalidator):
                    value = hvalidator(raw, ktype, kname)
                ## apply custom validation if defined
                hvalidator = getattr(self, 'clean__%s' % kname, None)
                if callable(hvalidator):
                    value = hvalidator(raw, value)

                setattr(self, kname, value)

    ##
    ## default validators
    ##
    def clean_numeric(self, raw, ktype, kname=None, defval=0):
        """
            Clean and convert input value to requested numeric type
            :param raw: raw input data
            :param ktype: variable type to which result should be casted
            :param defval: default value to be used in case of cast error
        """

        if isinstance(raw, ktype):
            return raw

        if isinstance(raw, basestring):
            raw = raw.strip()
        try:
            return ktype(raw)
        except:
            logger.warning('failed to convert data for key=%s, raw=%s to type=%s' % (kname, raw, ktype))
            return defval

    def clean_string(self, raw, ktype, kname=None, defval=""):
        """
            Clean and convert input value to requested string type
            :param raw: raw input data
            :param ktype: variable type to which result should be casted
            :param defval: default value to be used in case of cast error
        """

        if isinstance(raw, ktype):
            return raw

        if isinstance(raw, basestring):
            raw = raw.strip()
        elif raw is None:
            return defval
        try:
            return ktype(raw)
        except:
            logger.warning('failed to convert data for key=%s, raw=%s to type=%s' % (kname, raw, ktype))
            return defval

    def clean_boolean(self, raw, ktype, kname=None, defval=None):
        """
            Clean and convert input value to requested boolean type
            :param raw: raw input data
            :param ktype: variable type to which result should be casted
            :param defval: default value to be used in case of cast error
        """

        if isinstance(raw, ktype):
            return raw

        val = str(raw).strip().lower()
        allowed_values = ['', 'none', 'true', 'false', 'yes', 'no', '1', '0']

        if val not in allowed_values:
            logger.warning('failed to convert data for key=%s, raw=%s to type=%s' % (kname, raw, ktype))
            return defval

        return raw.lower() in ['1', 'true', 'yes']


    def clean_dictdata(self, raw, ktype, kname=None, defval=None):
        """
            Clean and convert input value to requested dict type
            :param raw: raw input data
            :param ktype: variable type to which result should be casted
            :param defval: default value to be used in case of cast error
        """

        if isinstance(raw, ktype):
            return raw

        elif raw is None:
            return defval
        try:
            return ktype(raw)
        except:
            logger.warning('failed to convert data for key=%s, raw=%s to type=%s' % (kname, raw, ktype))
            return defval


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

        return value * 60;


    def __repr__(self):

        ret = []
        attrs = [key for key in dir(self) if not callable(getattr(self, key)) and not key.startswith('_')]
        for key in sorted(attrs):
            ret.append(" %s=%s" % (key, getattr(self, key)))

        return '\n'.join(ret)
