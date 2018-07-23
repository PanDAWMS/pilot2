#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Wen Guan, wen.guan@cern,ch, 2018

# refactored by Alexey Anisenkov

import os
import hashlib
import logging

from pilot.info import infosys
from pilot.common.exception import PilotException, ErrorCodes
from pilot.util.filehandling import calculate_checksum


class StagingClient(object):
    """
        Base Staging Client
    """

    copytool_modules = {'rucio': {'module_name': 'rucio'},
                        'gfal': {'module_name': 'gfal'},
                        'gfalcopy': {'module_name': 'gfal'},
                        'lcgcp': {'module_name': 'lcgcp'},
                        'dccp': {'module_name': 'dccp'},
                        'xrdcp': {'module_name': 'xrdcp'},
                        'mv': {'module_name': 'mv'},
                        'lsm': {'module_name': 'lsm'},
                        'objectstore': {'module_name': 'objectstore'}
                        }

    direct_remoteinput_allowed_schemas = ['root']
    remoteinput_allowed_schemas = ['root', 'gsiftp', 'dcap', 'davs', 'srm']

    def __init__(self, infosys_instance=None, acopytools=None, logger=None, default_copytools='rucio'):
        """
            If `acopytools` is not specified then it will be automatically resolved via infosys. In this case `infosys` requires initialization.
            :param acopytools: dict of copytool names per activity to be used for transfers. Accepts also list of names or string value without activity passed.
            :param logger: logging.Logger object to use for logging (None means no logging)
            :param default_copytools: copytool name(s) to be used in case of unknown activity passed. Accepts either list of names or single string value.
        """

        super(StagingClient, self).__init__()

        if not logger:
            logger = logging.getLogger('%s.%s' % (__name__, 'null'))
            # logger.disabled = True

        self.logger = logger
        self.infosys = infosys_instance or infosys

        if isinstance(acopytools, basestring):
            acopytools = {'default': [acopytools]} if acopytools else {}
        if isinstance(acopytools, (list, tuple)):
            acopytools = {'default': acopytools} if acopytools else {}

        self.acopytools = acopytools
        if not self.acopytools:  ## resolve from queuedata.acopytools using infosys
            self.acopytools = (self.infosys.queuedata.acopytools or {}).copy()
        if not self.acopytools:  ## resolve from queuedata.copytools using infosys
            self.acopytools = dict(default=(self.infosys.queuedata.copytools or {}).keys())

        if not self.acopytools:
            logger.error('Failed to initilize StagingClient: no acopytools options found, acopytools=' % self.acopytools)
            raise PilotException("Failed to resolve acopytools settings")

        if not self.acopytools.get('default'):
            if isinstance(default_copytools, basestring):
                default_copytools = [default_copytools] if default_copytools else []
            self.acopytools['default'] = default_copytools

        logger.info('Configured copytools per activity: acopytools=%s' % self.acopytools)

        self.astorages = (self.infosys.queuedata.astorages or {}).copy()
        logger.info('Configured astorages per activity: astorages=%s' % self.astorages)

    def resolve_replicas(self, files):  # noqa: C901
        """
            Populates filespec.replicas for each entry from `files` list
            :param files: list of `FileSpec` objects
            fdat.replicas = [(ddmendpoint, replica, ddm_se, ddm_path)]
            :return: `files`
        """

        logger = self.logger  ## the function could be static if logger will be moved outside

        xfiles = []
        ddmconf = self.infosys.resolve_storage_data()

        for fdat in files:
            ddmdat = ddmconf.get(fdat.ddmendpoint)
            if not ddmdat:
                raise Exception("Failed to resolve input ddmendpoint by name=%s (from PanDA), please check configuration. fdat=%s" % (fdat.ddmendpoint, fdat))

            ## skip fdat if need for further workflow (e.g. to properly handle OS ddms)

            fdat.accessmode = 'copy'        ### quick hack to avoid changing logic below for DIRECT access handling  ## REVIEW AND FIX ME LATER
            fdat.allowRemoteInputs = False  ### quick hack to avoid changing logic below for DIRECT access handling  ## REVIEW AND FIX ME LATER

            if not fdat.inputddms and self.infosys.queuedata:
                fdat.inputddms = self.infosys.queuedata.astorages.get('pr', {})  ## FIX ME LATER: change to proper activity=read_lan
            xfiles.append(fdat)

        if not xfiles:  # no files for replica look-up
            return files

        # load replicas from Rucio
        from rucio.client import Client
        c = Client()

        ## for the time being until Rucio bug with geo-ip sorting is resolved
        ## do apply either simple query list_replicas() without geoip sort to resolve LAN replicas in case of directaccesstype=[None, LAN]
        # otherwise in case of directaccesstype=WAN mode do query geo sorted list_replicas() with location data passed

        bquery = {'schemes': ['srm', 'root', 'davs', 'gsiftp', 'https'],
                  'dids': [dict(scope=e.scope, name=e.lfn) for e in xfiles]}

        allow_remoteinput = True in set(e.allowRemoteInputs for e in xfiles)  ## implement direct access later

        try:
            query = bquery.copy()
            if allow_remoteinput:
                location = self.detect_client_location()
                if not location:
                    raise Exception("Failed to get client location for Rucio")
                query.update(sort='geoip', client_location=location)

            try:
                logger.info('Call rucio.list_replicas() with query=%s' % query)
                replicas = c.list_replicas(**query)
            except TypeError as e:
                if query == bquery:
                    raise
                logger.warning("Detected outdated Rucio list_replicas(), cannot do geoip-sorting: %s .. fallback to old list_replicas() call" % e)
                replicas = c.list_replicas(**bquery)

        except Exception as e:
            raise PilotException("Failed to get replicas from Rucio: %s" % e)  #, code=ErrorCodes.XX__FAILEDLFCGETREPS)

        replicas = list(replicas)
        logger.debug("replicas received from Rucio: %s" % replicas)

        files_lfn = dict(((e.scope, e.lfn), e) for e in xfiles)
        logger.debug("files_lfn=%s" % files_lfn)

        for r in replicas:
            k = r['scope'], r['name']
            fdat = files_lfn.get(k)
            if not fdat:  # not requested replica returned?
                continue

            fdat.replicas = []  # reset replicas list

            def get_preferred_replica(replicas, allowed_schemas):
                for schema in allowed_schemas:
                    for replica in replicas:
                        if replica and replica.startswith('%s://' % schema):
                            return replica
                return None

            has_direct_remoteinput_replicas = False

            # local replicas
            for ddm in fdat.inputddms:  ## iterate over local ddms and check if replica is exist here

                if ddm not in r['rses']:  # no replica found for given local ddm
                    continue

                fdat.replicas.append((ddm, r['rses'][ddm]))

                if not has_direct_remoteinput_replicas:
                    has_direct_remoteinput_replicas = bool(get_preferred_replica(r['rses'][ddm], self.direct_remoteinput_allowed_schemas))

            if (not fdat.replicas or (fdat.accessmode == 'direct' and not has_direct_remoteinput_replicas)) and fdat.allowRemoteInputs:
                if fdat.accessmode == 'direct':
                    allowed_schemas = self.direct_remoteinput_allowed_schemas
                else:
                    allowed_schemas = self.remoteinput_allowed_schemas

                if not fdat.replicas:
                    logger.info("No local replicas found for lfn=%s but allowRemoteInputs is set => looking for remote inputs" % fdat.lfn)
                else:
                    logger.info("Direct access is set but no local direct access files, but allowRemoteInputs is set => looking for remote inputs" % fdat.lfn)
                logger.info("consider first/closest replica, accessmode=%s, remoteinput_allowed_schemas=%s" % (fdat.accessmode, allowed_schemas))
                logger.debug('rses=%s' % r['rses'])
                for ddm, replicas in r['rses'].iteritems():
                    replica = get_preferred_replica(r['rses'][ddm], self.remoteinput_allowed_schemas)
                    if not replica:
                        continue

                    # remoteinput supported replica (root) replica has been found
                    fdat.replicas.append((ddm, r['rses'][ddm]))
                    # break # ignore other remote replicas/sites

            # verify filesize and checksum values
            if fdat.filesize != r['bytes']:
                logger.warning("Filesize of input file=%s mismatched with value from Rucio replica: filesize=%s, replica.filesize=%s, fdat=%s"
                               % (fdat.lfn, fdat.filesize, r['bytes'], fdat))
            if not fdat.filesize:
                fdat.filesize = r['bytes']
                logger.warning("Filesize value of input file=%s is not defined, assigning info got from Rucio replica: filesize=" % (fdat.lfn, r['bytes']))

            for ctype in ['adler32', 'md5']:
                if fdat.checksum.get(ctype) != r[ctype] and r[ctype]:
                    logger.warning("Checksum value of input file=%s mismatched with info got from Rucio replica: checksum=%s, replica.checksum=%s, fdat=%s"
                                   % (fdat.lfn, fdat.checksum, r[ctype], fdat))
                if not fdat.checksum.get(ctype) and r[ctype]:
                    fdat.checksum[ctype] = r[ctype]

        logger.info('Number of resolved replicas:\n' +
                    '\n'.join(["lfn=%s: replicas=%s, allowRemoteInputs=%s, is_directaccess=%s"
                               % (f.lfn, len(f.replicas), f.allowRemoteInputs, f.is_directaccess(ensure_replica=False)) for f in files]))

        return files

    def resolve_surl_os(self, fspec, protocol, ddm, **kwargs):
        """
            Get final destination SURL for file to be transferred to Objectstore
            Can be customized at the level of specific copytool
            :param protocol: suggested protocol
            :param ddm: ddm storage data
            :param fspec: file spec data
            :return: surl as a string.
        """

        # consider only deterministic sites (output destination)
        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), fspec.lfn)
        fspec.protocol_id = protocol.get('id', None)
        return surl

    @classmethod
    def detect_client_location(self, site):  ## TO BE DEPRECATED ONCE RUCIO BUG IS FIXED
        """
        Open a UDP socket to a machine on the internet, to get the local IP address
        of the requesting client.
        Try to determine the sitename automatically from common environment variables,
        in this order: SITE_NAME, ATLAS_SITE_NAME, OSG_SITE_NAME. If none of these exist
        use the fixed string 'ROAMING'.
        Note: this is a modified Rucio function.  ## TO BE DEPRECATED ONCE RUCIO BUG IS FIXED

        :return: expected location dict for Rucio functions: dict(ip, fqdn, site)
        """

        ret = {}
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            ret = {'ip': ip, 'fqdn': socket.getfqdn(), 'site': site}
        except Exception as e:
            #self.log('socket() failed to lookup local IP')
            print('socket() failed to lookup local IP: %s' % e)

        return ret

    def transfer_files(self, copytool, files, **kwargs):
        """
            Apply transfer of given `files` using passed `copytool` module
            Should be implemented by custom Staging Client
            :param copytool: copytool module
            :param files: list of `FileSpec` objects
            :param kwargs: extra kwargs to be passed to copytool transfer handler
            :raise: PilotException in case of controlled error
        """

        raise NotImplementedError

    def transfer(self, files, activity='default', **kwargs):  # noqa: C901
        """
            Automatically stage passed files using copy tools related to given `activity`
            :param files: list of `FileSpec` objects
            :param activity: list of activity names used to determine appropriate copytool (prioritized list)
            :param kwargs: extra kwargs to be passed to copytool transfer handler
            :raise: PilotException in case of controlled error
            :return: output of copytool trasfers (to be clarified)
        """

        if isinstance(activity, basestring):
            activity = [activity]
        if 'default' not in activity:
            activity.append('default')

        copytools = None
        current_activity = None
        for aname in activity:
            copytools = self.acopytools.get(aname)
            if copytools and aname in self.astorages.keys():
                current_activity = aname
                break

        if not copytools or not current_activity:
            raise PilotException('Failed to resolve copytool by preferred activities=%s, acopytools=%s' % (activity, self.acopytools))

        result, errors = None, []

        for name in copytools:

            try:
                if name not in self.copytool_modules:
                    raise PilotException('passed unknown copytool with name=%s .. skipped' % name)
                module = self.copytool_modules[name]['module_name']
                self.logger.info('Trying to use copytool=%s for activity=%s' % (name, current_activity))
                copytool = __import__('pilot.copytool.%s' % module, globals(), locals(), [module], -1)
            except PilotException as e:
                errors.append(e)
                self.logger.debug('Error: %s' % e)
                continue
            except Exception as e:
                self.logger.warning('Failed to import copytool module=%s, error=%s' % (module, e))
                self.logger.debug('Error: %s' % e)
                continue
            try:
                result = self.transfer_files(copytool, files, current_activity, **kwargs)
            except PilotException, e:
                errors.append(e)
                self.logger.debug('Error: %s' % e)
            except Exception as e:
                self.logger.warning('Failed to transfer files using copytool=%s .. skipped; error=%s' % (copytool, e))
                import traceback
                self.logger.error(traceback.format_exc())
                errors.append(e)

            if errors and isinstance(errors[-1], PilotException) and errors[-1].get_error_code() == ErrorCodes.MISSINGOUTPUTFILE:
                raise errors[-1]

            if result:
                break

        if not result:
            raise PilotException('Failed to transfer files using copytools=%s, error=%s' % (copytools, errors))

        return result


class StageInClient(StagingClient):

    def resolve_replica(self, fspec, allowed_schemas=None):
        """
            Resolve input replica according to allowed schema
            :param fspec: input `FileSpec` objects
            :param allowed_schemas: list of allowed schemas or any if None
            :return: dict(surl, ddmendpoint, pfn)
        """

        if not fspec.replicas:
            return
        allowed_schemas = allowed_schemas or [None]
        replica = None
        for sval in allowed_schemas:
            for ddmendpoint, replicas in fspec.replicas:
                if not replicas:  # ignore ddms with no replicas
                    continue
                surl = replicas[0]  # assume srm protocol is first entry
                self.logger.info("[stage-in] surl (srm replica) from Rucio: pfn=%s, ddmendpoint=%s" % (surl, ddmendpoint))
                for r in replicas:
                    if sval is None or r.startswith("%s://" % sval):
                        replica = r
                        break
                if replica:
                    break
            if replica:
                break

        if not replica:  # replica not found
            error = 'Failed to find replica for input file=%s, allowed_schemas=%s, fspec=%s' % (fspec.lfn, allowed_schemas, fspec)
            self.logger.error("resolve_replica: %s" % error)
            raise PilotException(error, code=ErrorCodes.REPLICANOTFOUND)

        return {'surl': surl, 'ddmendpoint': ddmendpoint, 'pfn': replica}

    def resolve_protocol(self, fspec, activity, ddm, allowed_schemas):
        """
            Resolve protocols according to allowed schema
            :param fspec: `FileSpec` instance
            :param activity: activity name
            :param ddm: ddm storage data
            :param allowed_schemas: list of allowed schemas or any if None
            :return: list of dict(endpoint, path, flavour)
        """

        if isinstance(activity, basestring):
            activity = [activity]

        protocols = []
        for aname in activity:
            if aname == 'es_events':
                aname = 'pw'
            if aname == 'es_events_read':
                aname = 'pr'
            protocols = ddm.arprotocols.get(aname)
            if protocols:
                break

        allow_protocols = []
        for schema in allowed_schemas:
            for pdat in protocols:
                if schema is None or pdat.get('endpoint', '').startswith("%s://" % schema):
                    allow_protocols.append(pdat)

        fspec.protocols = allow_protocols
        return fspec.protocols

    def get_ids_to_ddms(self, ddmconf):
        """
           Get a map from storage ids to ddmendpoints.
           :returns: a dict maps from ids to ddmendpoints.
        """
        ids_to_ddms = {}
        for ddmendpoint in ddmconf:
            ids_to_ddms[ddmconf[ddmendpoint].pk] = ddmendpoint
        return ids_to_ddms

    def fix_ddmendpoint_from_storage_id(self, files, activity=None, allowed_schemas=[]):
        """
            If storage_id is specified, fix ddmendpoint by parsing storage_id
        """
        require_fix_storage_id = False
        for fspec in files:
            if fspec.storage_id:
                require_fix_storage_id = True
                break
        if not require_fix_storage_id:
            return

        ddmconf = self.infosys.resolve_storage_data()
        ids_to_ddms = self.get_ids_to_ddms(ddmconf)
        for fspec in files:
            if fspec.storage_id:
                fspec.ddmendpoint = ids_to_ddms.get(fspec.storage_id, None)
                if not fspec.ddmendpoint:
                    raise PilotException('Failed to resolve ddmendpoint by storage_id=%s' % fspec.storage_id)
                ddm = ddmconf.get(fspec.ddmendpoint)
                if not ddm:
                    raise PilotException('Failed to resolve ddmendpoint by name=%s' % fspec.ddmendpoint)

                if not ddm.is_deterministic:
                    if ddm.type in ['OS_ES', 'OS_LOGS']:
                        protocols = self.resolve_protocol(fspec, activity, ddm, allowed_schemas)
                        if not protocols:
                            raise PilotException('Failed to resolve protocol for acitivty=%s, ddm=%s, allowed_schemas%s' % (activity,
                                                                                                                            fspec.ddmendpoint,
                                                                                                                            allowed_schemas))
                        protocol = protocols[0]
                        fspec.surl = self.resolve_surl_os(fspec, protocol, ddm)
                    else:
                        raise PilotException('resolve_surl(): Failed to construct SURL for non deterministic ddm=%s: NOT IMPLEMENTED', fspec.ddmendpoint)

    def transfer_files(self, copytool, files, activity=None, **kwargs):
        """
            Automatically stage in files using the selected copy tool module.

            :param copytool: copytool module
            :param files: list of `FileSpec` objects
            :param kwargs: extra kwargs to be passed to copytool transfer handler

            :return: the output of the copytool transfer operation
            :raise: PilotException in case of controlled error
        """

        if getattr(copytool, 'require_replicas', False) and files and files[0].replicas is None:
            files = self.resolve_replicas(files)
            allowed_schemas = getattr(copytool, 'allowed_schemas', None)
            for fspec in files:
                resolve_replica = getattr(copytool, 'resolve_replica', None)
                if not callable(resolve_replica):
                    resolve_replica = self.resolve_replica
                r = resolve_replica(fspec, allowed_schemas)
                if r.get('pfn'):
                    fspec.turl = r['pfn']
                if r.get('surl'):
                    fspec.surl = r['surl']  # TO BE CLARIFIED if it's still used and need
                if r.get('ddmendpoint'):
                    fspec.ddmendpoint = r['ddmendpoint']

                self.logger.info("[stage-in] found replica to be used for lfn=%s: ddmendpoint=%s, pfn=%s" % (fspec.lfn, fspec.ddmendpoint, fspec.turl))

        allowed_schemas = getattr(copytool, 'allowed_schemas', [])
        self.fix_ddmendpoint_from_storage_id(files, activity, allowed_schemas)

        if not copytool.is_valid_for_copy_in(files):
            self.logger.warning('Input is not valid for transfers using copytool=%s' % copytool)
            self.logger.debug('Input: %s' % files)
            raise PilotException('Invalid input data for transfer operation')

        if self.infosys:
            kwargs['copytools'] = self.infosys.queuedata.copytools
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()

        self.logger.info('Ready to transfer (stage-in) files: %s' % files)

        return copytool.copy_in(files, **kwargs)


class StageOutClient(StagingClient):

    def resolve_protocols(self, files, activity):
        """
            Populates filespec.protocols for each entry from `files` according to requested `activity`
            :param files: list of `FileSpec` objects
            :param activity: ordered list of preferred activity names to resolve SE protocols
            fdat.protocols = [dict(endpoint, path, flavour), ..]
            :return: `files`
        """

        ddmconf = self.infosys.resolve_storage_data()

        if isinstance(activity, basestring):
            activity = [activity]

        for fdat in files:
            ddm = ddmconf.get(fdat.ddmendpoint)
            if not ddm:
                raise Exception("Failed to resolve output ddmendpoint by name=%s (from PanDA), please check configuration. fdat=%s" % (fdat.ddmendpoint, fdat))

            protocols = []
            for aname in activity:
                if aname == 'es_events':
                    aname = 'pw'
                if aname == 'es_events_read':
                    aname = 'pr'
                protocols = ddm.arprotocols.get(aname)
                if protocols:
                    break

            fdat.protocols = protocols

        return files

    def resolve_protocol(self, fspec, allowed_schemas=None):
        """
            Resolve protocols according to allowed schema
            :param fspec: `FileSpec` instance
            :param allowed_schemas: list of allowed schemas or any if None
            :return: list of dict(endpoint, path, flavour)
        """

        if not fspec.protocols:
            return []

        protocols = []

        allowed_schemas = allowed_schemas or [None]
        for schema in allowed_schemas:
            for pdat in fspec.protocols:
                if schema is None or pdat.get('endpoint', '').startswith("%s://" % schema):
                    protocols.append(pdat)

        return protocols

    @classmethod
    def get_path(self, scope, lfn, prefix='rucio'):
        """
            Construct a partial Rucio PFN using the scope and the LFN
        """

        # <prefix=rucio>/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>

        hash_hex = hashlib.md5('%s:%s' % (scope, lfn)).hexdigest()

        #paths = [prefix] + scope.split('.') + [hash_hex[0:2], hash_hex[2:4], lfn]
        # exclude prefix from the path: this should be properly considered in protocol/AGIS for today
        paths = scope.split('.') + [hash_hex[0:2], hash_hex[2:4], lfn]
        paths = filter(None, paths)  # remove empty parts to avoid double /-chars

        return '/'.join(paths)

    def resolve_surl(self, fspec, protocol, ddmconf, **kwargs):
        """
            Get final destination SURL for file to be transferred
            Can be customized at the level of specific copytool
            :param protocol: suggested protocol
            :param ddmconf: full ddmconf data
            :param activity: ordered list of preferred activity names to resolve SE protocols
            :return: dict with keys ('pfn', 'ddmendpoint')
        """

        # consider only deterministic sites (output destination)

        ddm = ddmconf.get(fspec.ddmendpoint)
        if not ddm:
            raise PilotException('Failed to resolve ddmendpoint by name=%s' % fspec.ddmendpoint)

        if not ddm.is_deterministic:
            if ddm.type in ['OS_ES', 'OS_LOGS']:
                surl = self.resolve_surl_os(fspec, protocol, ddm)
            else:
                raise PilotException('resolve_surl(): Failed to construct SURL for non deterministic ddm=%s: NOT IMPLEMENTED', fspec.ddmendpoint)
        else:
            surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), self.get_path(fspec.scope, fspec.lfn))

        return {'surl': surl}

    def fill_storage_id(self, files):
        """
            Fill filespec storage_id for all files
        """
        ddmconf = self.infosys.resolve_storage_data()
        for fspec in files:
            if fspec.ddmendpoint:
                ddm = ddmconf.get(fspec.ddmendpoint)
                if ddm:
                    fspec.storage_id = ddm.pk

    def transfer_files(self, copytool, files, activity, **kwargs):
        """
            Automatically stage out files using the selected copy tool module.

            :param copytool: copytool module
            :param files: list of `FileSpec` objects
            :param activity: ordered list of preferred activity names to resolve SE protocols
            :param kwargs: extra kwargs to be passed to copytool transfer handler

            :return: the output of the copytool transfer operation
            :raise: PilotException in case of controlled error
        """

        # check if files exist before actual processing
        # populate filesize if need, calc checksum
        for fspec in files:
            self.logger.info("To transfer file(activity: %s): %s" % (activity, fspec))
            if not fspec.is_user_defined_ddmendpoint:
                if activity in self.astorages.keys():
                    fspec.ddmendpoint = self.astorages[activity][0]
                self.logger.info("Assign ddmendpoint to file: %s(%s)" % (fspec.ddmendpoint, activity))
            pfn = fspec.surl or getattr(fspec, 'pfn', None) or os.path.join(kwargs.get('workdir', ''), fspec.lfn)
            if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                msg = "Error: output pfn file does not exist: %s" % pfn
                self.logger.error(msg)
                raise PilotException(msg, code=ErrorCodes.MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")
            if not fspec.filesize:
                fspec.filesize = os.path.getsize(pfn)
            fspec.surl = pfn
            fspec.activity = activity
            if not fspec.checksum.get('adler32'):
                fspec.checksum['adler32'] = calculate_checksum(pfn)

        # prepare files (resolve protocol/transfer url)
        if getattr(copytool, 'require_protocols', True) and files:

            ddmconf = self.infosys.resolve_storage_data()
            allowed_schemas = getattr(copytool, 'allowed_schemas', None)
            files = self.resolve_protocols(files, activity)

            for fspec in files:

                protocols = self.resolve_protocol(fspec, allowed_schemas)
                if not protocols:  #  no protocols found
                    error = 'Failed to resolve protocol for file=%s, allowed_schemas=%s, fspec=%s' % (fspec.lfn, allowed_schemas, fspec)
                    self.logger.error("resolve_protocol: %s" % error)
                    raise PilotException(error, code=ErrorCodes.NOSTORAGEPROTOCOL)

                # take first available protocol for copytool: FIX ME LATER if need (do iterate over all allowed protocols?)
                protocol = protocols[0]

                self.logger.info("Resolved protocol to be used for transfer: data=%s" % protocol)

                resolve_surl = getattr(copytool, 'resolve_surl', None)
                if not callable(resolve_surl):
                    resolve_surl = self.resolve_surl

                r = resolve_surl(fspec, protocol, ddmconf, activity=activity)  ## pass ddmconf & activity for possible custom look up at the level of copytool
                if r.get('surl'):
                    fspec.turl = r['surl']
                if r.get('ddmendpoint'):
                    fspec.ddmendpoint = r['ddmendpoint']

        if not copytool.is_valid_for_copy_out(files):
            self.logger.warning('Input is not valid for transfers using copytool=%s' % copytool)
            self.logger.debug('Input: %s' % files)
            raise PilotException('Invalid input for transfer operation')

        self.fill_storage_id(files)

        self.logger.info('Ready to transfer (stage-out) files: %s' % files)

        if self.infosys:
            kwargs['copytools'] = self.infosys.queuedata.copytools
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()

            # some copytools will need to know endpoint specifics (e.g. the space token) stored in ddmconf, add it
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()

        return copytool.copy_out(files, **kwargs)

#class StageInClientAsync(object):
#
#    def __init__(self, site):
#        raise NotImplementedError
#
#    def queue(self, files):
#        raise NotImplementedError
#
#    def is_transferring(self):
#        raise NotImplementedError
#
#    def start(self):
#        raise NotImplementedError
#
#    def finish(self):
#        raise NotImplementedError
#
#    def status(self):
#        raise NotImplementedError
#
#
#class StageOutClientAsync(object):
#
#    def __init__(self, site):
#        raise NotImplementedError
#
#    def queue(self, files):
#        raise NotImplementedError
#
#    def is_transferring(self):
#        raise NotImplementedError
#
#    def start(self):
#        raise NotImplementedError
#
#    def finish(self):
#        raise NotImplementedError
#
#    def status(self):
#        raise NotImplementedError
