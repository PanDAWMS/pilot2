#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018

# refactored by Alexey Anisenkov

import os
import hashlib
import logging
import time

from pilot.info import infosys
from pilot.common.exception import PilotException, ErrorCodes, SizeTooLarge, NoLocalSpace, ReplicasNotFound
from pilot.util.filehandling import calculate_checksum
from pilot.util.math import convert_mb_to_b
from pilot.util.parameters import get_maximum_input_sizes
from pilot.util.workernode import get_local_disk_space
from pilot.util.timer import TimeoutException
from pilot.util.tracereport import TraceReport

errors = ErrorCodes()


class StagingClient(object):
    """
        Base Staging Client
    """

    mode = ""  # stage-in/out, set by the inheritor of the class
    copytool_modules = {'rucio': {'module_name': 'rucio'},
                        'gfal': {'module_name': 'gfal'},
                        'gfalcopy': {'module_name': 'gfal'},
                        'xrdcp': {'module_name': 'xrdcp'},
                        'mv': {'module_name': 'mv'},
                        'lsm': {'module_name': 'lsm'}
                        }

    # list of allowed schemas to be used for direct acccess mode from REMOTE replicas
    direct_remoteinput_allowed_schemas = ['root']
    # list of schemas to be used for direct acccess mode from LOCAL replicas
    direct_localinput_allowed_schemas = ['root', 'dcache', 'dcap', 'file', 'https']
    # list of allowed schemas to be used for transfers from REMOTE sites
    remoteinput_allowed_schemas = ['root', 'gsiftp', 'dcap', 'davs', 'srm', 'storm']

    def __init__(self, infosys_instance=None, acopytools=None, logger=None, default_copytools='rucio', trace_report=None):
        """
            If `acopytools` is not specified then it will be automatically resolved via infosys. In this case `infosys` requires initialization.
            :param acopytools: dict of copytool names per activity to be used for transfers. Accepts also list of names or string value without activity passed.
            :param logger: logging.Logger object to use for logging (None means no logging)
            :param default_copytools: copytool name(s) to be used in case of unknown activity passed. Accepts either list of names or single string value.
        """

        super(StagingClient, self).__init__()

        if not logger:
            logger = logging.getLogger('%s.%s' % (__name__, 'null'))
            logger.disabled = True

        self.logger = logger
        self.infosys = infosys_instance or infosys

        if isinstance(acopytools, basestring):
            acopytools = {'default': [acopytools]} if acopytools else {}
        if isinstance(acopytools, (list, tuple)):
            acopytools = {'default': acopytools} if acopytools else {}

        self.acopytools = acopytools or {}

        if self.infosys.queuedata:
            if not self.acopytools:  ## resolve from queuedata.acopytools using infosys
                self.acopytools = (self.infosys.queuedata.acopytools or {}).copy()
            if not self.acopytools:  ## resolve from queuedata.copytools using infosys
                self.acopytools = dict(default=(self.infosys.queuedata.copytools or {}).keys())

        if not self.acopytools.get('default'):
            if isinstance(default_copytools, basestring):
                default_copytools = [default_copytools] if default_copytools else []
            self.acopytools['default'] = default_copytools

        if not self.acopytools:
            msg = 'failed to initilize StagingClient: no acopytools options found, acopytools=%s' % self.acopytools
            logger.error(msg)
            self.trace_report.update(clientState='BAD_COPYTOOL', stateReason=msg)
            self.trace_report.send()
            raise PilotException("failed to resolve acopytools settings")
        logger.info('configured copytools per activity: acopytools=%s' % self.acopytools)

        # get an initialized trace report (has to be updated for get/put if not defined before)
        self.trace_report = trace_report if trace_report else TraceReport(pq=os.environ.get('PILOT_SITENAME', ''))

    @classmethod
    def get_preferred_replica(self, replicas, allowed_schemas):
        """
            Get preferred replica from the `replicas` list suitable for `allowed_schemas`
            :return: replica or None if not found
        """

        for replica in replicas:
            for schema in allowed_schemas:
                if replica and (not schema or replica.startswith('%s://' % schema)):
                    return replica

    def prepare_sources(self, files, activities=None):
        """
            Customize/prepare source data for each entry in `files` optionally checking data for requested `activities`
            (custom StageClient could extend the logic if need)
            :param files: list of `FileSpec` objects to be processed
            :param activities: string or ordered list of activities to resolve `astorages` (optional)
            :return: None
        """

        return

    def prepare_inputddms(self, files, activities=None):
        """
            Populates filespec.inputddms for each entry from `files` list
            :param files: list of `FileSpec` objects
            :param activities: sting or ordered list of activities to resolve astorages (optional)
            :return: None
        """

        activities = activities or 'read_lan'
        if isinstance(activities, basestring):
            activities = [activities]

        astorages = self.infosys.queuedata.astorages if self.infosys and self.infosys.queuedata else {}

        storages = []
        for a in activities:
            storages = astorages.get(a, [])
            if storages:
                break

        #activity = activities[0]
        #if not storages:  ## ignore empty astorages
        #    raise PilotException("Failed to resolve input sources: no associated storages defined for activity=%s (%s)"
        #                         % (activity, ','.join(activities)), code=ErrorCodes.NOSTORAGE, state='NO_ASTORAGES_DEFINED')

        for fdat in files:
            if not fdat.inputddms:
                fdat.inputddms = storages
            if not fdat.inputddms and fdat.ddmendpoint:
                fdat.inputddms = [fdat.ddmendpoint]

    def resolve_replicas(self, files):  # noqa: C901
        """
            Populates filespec.replicas for each entry from `files` list
            :param files: list of `FileSpec` objects
            fdat.replicas = [(ddmendpoint, replica, ddm_se, ddm_path)]
            :return: `files`
        """

        logger = self.logger
        xfiles = []
        #ddmconf = self.infosys.resolve_storage_data()

        for fdat in files:
            #ddmdat = ddmconf.get(fdat.ddmendpoint)
            #if not ddmdat:
            #    raise Exception("Failed to resolve input ddmendpoint by name=%s (from PanDA), please check configuration. fdat=%s" % (fdat.ddmendpoint, fdat))

            ## skip fdat if need for further workflow (e.g. to properly handle OS ddms)

            #fdat.accessmode = 'copy'        ### quick hack to avoid changing logic below for DIRECT access handling  ## REVIEW AND FIX ME LATER
            #fdat.allowremoteinputs = False  ### quick hack to avoid changing logic below for DIRECT access handling  ## REVIEW AND FIX ME LATER
            xfiles.append(fdat)

        if not xfiles:  # no files for replica look-up
            return files

        # load replicas from Rucio
        from rucio.client import Client
        c = Client()

        ## for the time being until Rucio bug with geo-ip sorting is resolved
        ## do apply either simple query list_replicas() without geoip sort to resolve LAN replicas in case of directaccesstype=[None, LAN]
        # otherwise in case of directaccesstype=WAN mode do query geo sorted list_replicas() with location data passed

        bquery = {'schemes': ['srm', 'root', 'davs', 'gsiftp', 'https', 'storm'],
                  'dids': [dict(scope=e.scope, name=e.lfn) for e in xfiles]}

        #allow_remoteinput = True in set(e.allowremoteinputs for e in xfiles)  ## implement direct access later

        try:
            query = bquery.copy()
            location = self.detect_client_location()
            if not location:
                raise PilotException("Failed to get client location for Rucio", code=errors.RUCIOLOCATIONFAILED)

            query.update(sort='geoip', client_location=location)
            logger.info('calling rucio.list_replicas() with query=%s' % query)
            replicas = c.list_replicas(**query)
        except Exception as e:
            raise PilotException("Failed to get replicas from Rucio: %s" % e, code=errors.RUCIOLISTREPLICASFAILED)

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
            has_direct_localinput_replicas = False

            # manually sort replicas by priority value since Rucio has a bug in ordering
            # .. can be removed once Rucio server-side fix will be delivered
            ordered_replicas = {}
            for pfn, xdat in sorted(r.get('pfns', {}).iteritems(), key=lambda x: x[1]['priority']):
                ordered_replicas.setdefault(xdat.get('rse'), []).append(pfn)

            # local replicas
            for ddm in fdat.inputddms:  ## iterate over local ddms and check if replica is exist here
                #pfns = r.get('rses', {}).get(ddm)  ## use me once Rucio bug is resolved
                pfns = ordered_replicas.get(ddm)    ## quick workaround of Rucio bug: use manually sorted pfns

                if not pfns:  # no replica found for given local ddm
                    continue

                fdat.replicas.append((ddm, pfns))

                if not has_direct_localinput_replicas:
                    has_direct_localinput_replicas = bool(self.get_preferred_replica(pfns, self.direct_localinput_allowed_schemas))

            if (not fdat.replicas or (fdat.accessmode == 'direct' and not has_direct_localinput_replicas)) and fdat.allowremoteinputs:
                if fdat.accessmode == 'direct':
                    allowed_schemas = self.direct_remoteinput_allowed_schemas
                else:
                    allowed_schemas = self.remoteinput_allowed_schemas

                if not fdat.replicas:
                    logger.info("No local replicas found for lfn=%s but allowremoteinputs is set => looking for remote inputs" % fdat.lfn)
                else:
                    logger.info("Direct access=True but no appr. local replicas found: allowremoteinputs=True =>checking remote input suitable for direct read")
                logger.info("Consider first/closest replica, accessmode=%s, allowed_schemas=%s" % (fdat.accessmode, allowed_schemas))
                #logger.debug('rses=%s' % r['rses'])
                for ddm, pfns in r['rses'].iteritems():
                    replica = self.get_preferred_replica(pfns, allowed_schemas)
                    if not replica:
                        continue

                    # remoteinput supported replica found
                    fdat.replicas.append((ddm, pfns))
                    # break # ignore other remote replicas/sites

            # verify filesize and checksum values
            self.trace_report.update(validateStart=time.time())
            status = True
            if fdat.filesize != r['bytes']:
                logger.warning("Filesize of input file=%s mismatched with value from Rucio replica: filesize=%s, replica.filesize=%s, fdat=%s"
                               % (fdat.lfn, fdat.filesize, r['bytes'], fdat))
                status = False

            if not fdat.filesize:
                fdat.filesize = r['bytes']
                logger.warning("Filesize value for input file=%s is not defined, assigning info from Rucio replica: filesize=%s" % (fdat.lfn, r['bytes']))

            for ctype in ['adler32', 'md5']:
                if fdat.checksum.get(ctype) != r[ctype] and r[ctype]:
                    logger.warning("Checksum value of input file=%s mismatched with info got from Rucio replica: checksum=%s, replica.checksum=%s, fdat=%s"
                                   % (fdat.lfn, fdat.checksum, r[ctype], fdat))
                    status = False

                if not fdat.checksum.get(ctype) and r[ctype]:
                    fdat.checksum[ctype] = r[ctype]

            if not status:
                logger.info("filesize and checksum verification done")
                self.trace_report.update(clientState="DONE")

        logger.info('Number of resolved replicas:\n' +
                    '\n'.join(["lfn=%s: replicas=%s, allowremoteinputs=%s, is_directaccess=%s"
                               % (f.lfn, len(f.replicas), f.allowremoteinputs, f.is_directaccess(ensure_replica=False)) for f in files]))

        return files

    @classmethod
    def detect_client_location(self):
        """
        Open a UDP socket to a machine on the internet, to get the local IPv4 and IPv6
        addresses of the requesting client.
        Try to determine the sitename automatically from common environment variables,
        in this order: SITE_NAME, ATLAS_SITE_NAME, OSG_SITE_NAME. If none of these exist
        use the fixed string 'ROAMING'.
        """

        ip = '0.0.0.0'
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            pass

        ip6 = '::'
        try:
            s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            s.connect(("2001:4860:4860:0:0:0:0:8888", 80))
            ip6 = s.getsockname()[0]
        except Exception:
            pass

        site = os.environ.get('PILOT_RUCIO_SITENAME', 'unknown')
#        site = os.environ.get('SITE_NAME',
#                              os.environ.get('ATLAS_SITE_NAME',
#                                             os.environ.get('OSG_SITE_NAME',
#                                                            'ROAMING')))

        return {'ip': ip,
                'ip6': ip6,
                'fqdn': socket.getfqdn(),
                'site': site}

    def transfer_files(self, copytool, files, **kwargs):
        """
            Apply transfer of given `files` using passed `copytool` module
            Should be implemented by custom Staging Client
            :param copytool: copytool module
            :param files: list of `FileSpec` objects
            :param kwargs: extra kwargs to be passed to copytool transfer handler
            :raise: PilotException in case of controlled error
        """

        raise NotImplemented()

    def transfer(self, files, activity='default', **kwargs):  # noqa: C901
        """
            Automatically stage passed files using copy tools related to given `activity`
            :param files: list of `FileSpec` objects
            :param activity: list of activity names used to determine appropriate copytool (prioritized list)
            :param kwargs: extra kwargs to be passed to copytool transfer handler
            :raise: PilotException in case of controlled error
            :return: output of copytool transfers (to be clarified)
        """

        self.trace_report.update(relativeStart=time.time(), transferStart=time.time())

        if isinstance(activity, basestring):
            activity = [activity]
        if 'default' not in activity:
            activity.append('default')

        copytools = None
        for aname in activity:
            copytools = self.acopytools.get(aname)
            if copytools:
                break

        if not copytools:
            raise PilotException('failed to resolve copytool by preferred activities=%s, acopytools=%s' %
                                 (activity, self.acopytools))

        # populate inputddms if need
        self.prepare_inputddms(files)

        # initialize ddm_activity name for requested files if not set
        for fspec in files:
            if fspec.ddm_activity:  # skip already initialized data
                continue
            if self.mode == 'stage-in':
                fspec.ddm_activity = filter(None, ['read_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'read_wan'])
            else:
                fspec.ddm_activity = filter(None, ['write_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'write_wan'])

        result, caught_errors = None, []

        for name in copytools:

            try:
                if name not in self.copytool_modules:
                    raise PilotException('passed unknown copytool with name=%s .. skipped' % name,
                                         code=ErrorCodes.UNKNOWNCOPYTOOL)

                module = self.copytool_modules[name]['module_name']
                self.logger.info('trying to use copytool=%s for activity=%s' % (name, activity))
                copytool = __import__('pilot.copytool.%s' % module, globals(), locals(), [module], -1)
                self.trace_report.update(protocol=name)

            except PilotException as e:
                caught_errors.append(e)
                self.logger.debug('error: %s' % e)
                continue
            except Exception as e:
                self.logger.warning('failed to import copytool module=%s, error=%s' % (module, e))
                continue
            try:
                result = self.transfer_files(copytool, files, activity, **kwargs)
            except PilotException as e:
                msg = 'failed to execute transfer_files(): PilotException caught: %s' % e
                self.logger.warning(msg)
                caught_errors.append(e)
            except TimeoutException as e:
                msg = 'function timed out: %s' % e
                self.logger.warning(msg)
                caught_errors.append(e)
            except Exception as e:
                self.logger.warning('failed to transfer files using copytool=%s .. skipped; error=%s' % (copytool, e))
                import traceback
                self.logger.error(traceback.format_exc())
                caught_errors.append(e)
            else:
                self.logger.debug('transfer_files() completed with result=%s' % str(result))

            if caught_errors and isinstance(caught_errors[-1], PilotException) and \
                    caught_errors[-1].get_error_code() == ErrorCodes.MISSINGOUTPUTFILE:
                raise caught_errors[-1]

            if result:
                break

        if not result:
            # Propagate message from first error back up
            errmsg = str(caught_errors[0]) if caught_errors else ''
            if caught_errors and "Cannot authenticate" in str(caught_errors):
                code = ErrorCodes.STAGEINAUTHENTICATIONFAILURE
            elif caught_errors and "bad queue configuration" in str(caught_errors):
                code = ErrorCodes.BADQUEUECONFIGURATION
            elif caught_errors and isinstance(caught_errors[0], PilotException):
                code = caught_errors[0].get_error_code()
                errmsg = caught_errors[0].get_last_error()
            elif caught_errors and isinstance(caught_errors[0], TimeoutException):
                code = errors.STAGEINTIMEOUT if self.mode == 'stage-in' else errors.STAGEOUTTIMEOUT  # is it stage-in/out?
                self.logger.warning('caught time-out exception: %s' % caught_errors[0])
            else:
                code = errors.STAGEINFAILED if self.mode == 'stage-in' else errors.STAGEOUTFAILED  # is it stage-in/out?
            details = str(caught_errors) + ":" + 'failed to transfer files using copytools=%s' % copytools
            self.logger.fatal(details)
            raise PilotException(details, code=code)

        self.logger.debug('result=%s' % str(result))
        return result

    def require_protocols(self, files, copytool, activity):
        """
            Populates fspec.protocols and fspec.turl for each entry in `files` according to preferred fspec.ddm_activity
            :param files: list of `FileSpec` objects
            :param activity: str or ordered list of transfer activity names to resolve acopytools related data
            :return: None
        """

        allowed_schemas = getattr(copytool, 'allowed_schemas', None)

        if self.infosys and self.infosys.queuedata:
            copytool_name = copytool.__name__.rsplit('.', 1)[-1]
            allowed_schemas = self.infosys.queuedata.resolve_allowed_schemas(activity, copytool_name) or allowed_schemas

        files = self.resolve_protocols(files)
        ddmconf = self.infosys.resolve_storage_data()

        for fspec in files:

            protocols = self.resolve_protocol(fspec, allowed_schemas)
            if not protocols:  #  no protocols found
                error = 'Failed to resolve protocol for file=%s, allowed_schemas=%s, fspec=%s' % (fspec.lfn, allowed_schemas, fspec)
                self.logger.error("resolve_protocol: %s" % error)
                raise PilotException(error, code=ErrorCodes.NOSTORAGEPROTOCOL)

            # take first available protocol for copytool: FIX ME LATER if need (do iterate over all allowed protocols?)
            protocol = protocols[0]

            self.logger.info("Resolved protocol to be used for transfer lfn=%s: data=%s" % (protocol, fspec.lfn))

            resolve_surl = getattr(copytool, 'resolve_surl', None)
            if not callable(resolve_surl):
                resolve_surl = self.resolve_surl

            r = resolve_surl(fspec, protocol, ddmconf)  ## pass ddmconf for possible custom look up at the level of copytool
            if r.get('surl'):
                fspec.turl = r['surl']
            if r.get('ddmendpoint'):
                fspec.ddmendpoint = r['ddmendpoint']

    def resolve_protocols(self, files):
        """
            Populates filespec.protocols for each entry from `files` according to preferred `fspec.ddm_activity` value
            :param files: list of `FileSpec` objects
            fdat.protocols = [dict(endpoint, path, flavour), ..]
            :return: `files`
        """

        ddmconf = self.infosys.resolve_storage_data()

        for fdat in files:
            ddm = ddmconf.get(fdat.ddmendpoint)
            if not ddm:
                error = 'Failed to resolve output ddmendpoint by name=%s (from PanDA), please check configuration.' % fdat.ddmendpoint
                self.logger.error("resolve_protocols: %s, fspec=%s" % (error, fdat))
                raise PilotException(error, code=ErrorCodes.NOSTORAGE)

            protocols = []
            for aname in fdat.ddm_activity:
                protocols = ddm.arprotocols.get(aname)
                if protocols:
                    break

            fdat.protocols = protocols

        return files

    @classmethod
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


class StageInClient(StagingClient):

    mode = "stage-in"

    def resolve_replica(self, fspec, primary_schemas=None, allowed_schemas=None):
        """
            Resolve input replica first according to `primary_schemas`,
            if not found then look up within `allowed_schemas`
            :param fspec: input `FileSpec` objects
            :param allowed_schemas: list of allowed schemas or any if None
            :return: dict(surl, ddmendpoint, pfn)
            :raise PilotException: if replica not found
        """

        if not fspec.replicas:
            self.logger.warning('resolve_replicas() received no fspec.replicas')
            return

        allowed_schemas = allowed_schemas or [None]
        replica = None

        for ddmendpoint, replicas in fspec.replicas:
            if not replicas:  # ignore ddms with no replicas
                continue
            if primary_schemas:  ## look up primary schemas if requested
                replica = self.get_preferred_replica(replicas, primary_schemas)
            if not replica:
                replica = self.get_preferred_replica(replicas, allowed_schemas)
            if replica:
                surl = self.get_preferred_replica(replicas, ['srm']) or replicas[0]  # prefer SRM protocol for surl -- to be verified
                self.logger.info("[stage-in] surl (srm replica) from Rucio: pfn=%s, ddmendpoint=%s" % (surl, ddmendpoint))
                break

        if not replica:  # replica not found
            schemas = 'any' if not allowed_schemas[0] else ','.join(allowed_schemas)
            error = 'Failed to find replica for input file=%s, allowed_schemas=%s, fspec=%s' % (fspec.lfn, schemas, fspec)
            self.logger.error("resolve_replica: %s" % error)
            raise PilotException(error, code=ErrorCodes.REPLICANOTFOUND)

        return {'surl': surl, 'ddmendpoint': ddmendpoint, 'pfn': replica}

    def get_direct_access_variables(self, job):
        """
        Return the direct access settings for the PQ.

        :param job: job object.
        :return: allow_direct_access (bool), direct_access_type (string).
        """

        allow_direct_access, direct_access_type = False, ''
        if self.infosys.queuedata:  ## infosys is initialized
            allow_direct_access = self.infosys.queuedata.direct_access_lan or self.infosys.queuedata.direct_access_wan
            if self.infosys.queuedata.direct_access_lan:
                direct_access_type = 'LAN'
            if self.infosys.queuedata.direct_access_wan:
                direct_access_type = 'WAN'
        else:
            self.logger.info('infosys.queuedata is not initialized: direct access mode will be DISABLED by default')

        if job and not job.is_analysis() and job.transfertype != 'direct':  ## task forbids direct access
            allow_direct_access = False
            self.logger.info('switched off direct access mode for production job since transfertype=%s' % job.transfertype)

        return allow_direct_access, direct_access_type

    def set_accessmodes_for_direct_access(self, files, direct_access_type):
        """
        Update the FileSpec accessmodes for direct access and sort the files to get candidates for remote_io coming
        first in order to exclude them from checking of available space for stage-in.

        :param files: FileSpec objects.
        :param direct_access_type: type of direct access (LAN or WAN) (string).
        :return:
        """

        # sort the files
        files = sorted(files, key=lambda x: x.is_directaccess(ensure_replica=False), reverse=True)

        # populate allowremoteinputs for each FileSpec object
        for fdata in files:
            is_directaccess = fdata.is_directaccess(ensure_replica=False)
            if is_directaccess and direct_access_type == 'WAN':  ## is it the same for ES workflow ?? -- test and verify/FIXME LATER
                fdata.allowremoteinputs = True
            self.logger.info("check direct access for lfn=%s: allow_direct_access=true, fdata.is_directaccess()=%s =>"
                             " is_directaccess=%s, allowremoteinputs=%s" % (fdata.lfn,
                                                                            fdata.is_directaccess(ensure_replica=False),
                                                                            is_directaccess, fdata.allowremoteinputs))
            # must update accessmode for user jobs (it is only set already for production jobs)
            if fdata.accessmode != 'direct' and is_directaccess and fdata.accessmode != 'copy':
                fdata.accessmode = 'direct'

            # reset accessmode if direct access is not to be used
            if fdata.accessmode == 'direct' and not is_directaccess:
                fdata.accessmode = ''

            self.logger.info('accessmode for LFN=%s: %s (is_directaccess=%s)' %
                             (fdata.lfn, fdata.accessmode, is_directaccess))

    def transfer_files(self, copytool, files, activity=None, **kwargs):
        """
        Automatically stage in files using the selected copy tool module.

        :param copytool: copytool module
        :param files: list of `FileSpec` objects
        :param kwargs: extra kwargs to be passed to copytool transfer handler

        :return: the output of the copytool transfer operation
        :raise: PilotException in case of controlled error
        """

        #logger.debug('trace_report[eventVersion]=%s' % self.trace_report.get('eventVersion', 'unknown'))

        # sort out direct access logic
        job = kwargs.get('job', None)
        #job_access_mode = job.accessmode if job else ''
        allow_direct_access, direct_access_type = self.get_direct_access_variables(job)
        self.logger.info("direct access settings for the PQ: allow_direct_access=%s (type=%s)" %
                         (allow_direct_access, direct_access_type))
        kwargs['allow_direct_access'] = allow_direct_access

        if allow_direct_access:
            self.set_accessmodes_for_direct_access(files, direct_access_type)

        if getattr(copytool, 'require_replicas', False) and files:
            if files[0].replicas is None:  ## look up replicas only once
                files = self.resolve_replicas(files)

            allowed_schemas = getattr(copytool, 'allowed_schemas', None)

            if self.infosys and self.infosys.queuedata:
                copytool_name = copytool.__name__.rsplit('.', 1)[-1]
                allowed_schemas = self.infosys.queuedata.resolve_allowed_schemas(activity, copytool_name) or allowed_schemas

            for fspec in files:
                resolve_replica = getattr(copytool, 'resolve_replica', None)
                resolve_replica = self.resolve_replica if not callable(resolve_replica) else resolve_replica

                ## prepare schemas which will be used to look up first the replicas allowed for direct access mode
                primary_schemas = self.direct_localinput_allowed_schemas if fspec.accessmode == 'direct' else None
                r = resolve_replica(fspec, primary_schemas, allowed_schemas)
                if not r:
                    raise ReplicasNotFound('resolve_replica() returned no replicas')

                if r.get('pfn'):
                    fspec.turl = r['pfn']
                if r.get('surl'):
                    fspec.surl = r['surl']  # TO BE CLARIFIED if it's still used and need
                if r.get('ddmendpoint'):
                    fspec.ddmendpoint = r['ddmendpoint']

                self.logger.info("[stage-in] found replica to be used for lfn=%s: ddmendpoint=%s, pfn=%s" %
                                 (fspec.lfn, fspec.ddmendpoint, fspec.turl))

        # prepare files (resolve protocol/transfer url)
        if getattr(copytool, 'require_input_protocols', False) and files:
            self.require_protocols(files, copytool, activity)

        if not copytool.is_valid_for_copy_in(files):
            msg = 'input is not valid for transfers using copytool=%s' % copytool
            self.logger.warning(msg)
            self.logger.debug('input: %s' % files)
            self.trace_report.update(clientState='NO_REPLICA', stateReason=msg)
            self.trace_report.send()
            raise PilotException('invalid input data for transfer operation')

        if self.infosys:
            if self.infosys.queuedata:
                kwargs['copytools'] = self.infosys.queuedata.copytools
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()
        kwargs['activity'] = activity

        # mark direct access files with status=remote_io
        if allow_direct_access:
            self.set_status_for_direct_access(files)

        # verify file sizes and available space for stage-in
        self.check_availablespace([e for e in files if e.status not in ['remote_io', 'transferred']])

        # add the trace report
        kwargs['trace_report'] = self.trace_report
        self.logger.info('ready to transfer (stage-in) files: %s' % files)

        return copytool.copy_in(files, **kwargs)

    def set_status_for_direct_access(self, files):
        """
        Update the FileSpec status with 'remote_io' for direct access mode.

        :param files: FileSpec objects.
        :return:
        """

        for fspec in files:
            if fspec.is_directaccess(ensure_replica=False):
                fspec.status_code = 0
                fspec.status = 'remote_io'

    def check_availablespace(self, files):
        """
        Verify that enough local space is available to stage in and run the job

        :param files: list of FileSpec objects.
        :raise: PilotException in case of not enough space or total input size too large
        """

        for f in files:
            self.logger.debug('lfn=%s filesize=%d accessmode=%s' % (f.lfn, f.filesize, f.accessmode))

        maxinputsize = convert_mb_to_b(get_maximum_input_sizes())
        totalsize = reduce(lambda x, y: x + y.filesize, files, 0)

        # verify total filesize
        if maxinputsize and totalsize > maxinputsize:
            error = "too many/too large input files (%s). total file size=%s B > maxinputsize=%s B" % \
                    (len(files), totalsize, maxinputsize)
            raise SizeTooLarge(error)

        self.logger.info("total input file size=%s B within allowed limit=%s B (zero value means unlimited)" %
                         (totalsize, maxinputsize))

        # get available space
        available_space = convert_mb_to_b(get_local_disk_space(os.getcwd()))
        self.logger.info("locally available space: %d B" % available_space)

        # are we within the limit?
        if totalsize > available_space:
            error = "not enough local space for staging input files and run the job (need %d B, but only have %d B)" % \
                    (totalsize, available_space)
            raise NoLocalSpace(error)


class StageOutClient(StagingClient):

    mode = "stage-out"

    def prepare_destinations(self, files, activities):
        """
            Resolve destination RSE (filespec.ddmendpoint) for each entry from `files` according to requested `activities`
            Apply Pilot-side logic to choose proper destination
            :param files: list of FileSpec objects to be processed
            :param activities: ordered list of activities to be used to resolve astorages
            :return: updated fspec entries
        """

        if not self.infosys.queuedata:  ## infosys is not initialized: not able to fix destination if need, nothing to do
            return files

        if isinstance(activities, (str, unicode)):
            activities = [activities]

        if not activities:
            raise PilotException("Failed to resolve destination: passed empty activity list. Internal error.",
                                 code=ErrorCodes.INTERNALPILOTPROBLEM, state='INTERNAL_ERROR')

        astorages = self.infosys.queuedata.astorages or {}

        storages = None
        activity = activities[0]
        for a in activities:
            storages = astorages.get(a, {})
            if storages:
                break

        if not storages:
            raise PilotException("Failed to resolve destination: no associated storages defined for activity=%s (%s)"
                                 % (activity, ','.join(activities)), code=ErrorCodes.NOSTORAGE, state='NO_ASTORAGES_DEFINED')

        # take the fist choice for now, extend the logic later if need
        ddm = storages[0]

        self.logger.info("[prepare_destinations][%s]: allowed (local) destinations: %s" % (activity, storages))
        self.logger.info("[prepare_destinations][%s]: resolved default destination ddm=%s" % (activity, ddm))

        for e in files:
            if not e.ddmendpoint:  ## no preferences => use default destination
                self.logger.info("[prepare_destinations][%s]: fspec.ddmendpoint is not set for lfn=%s"
                                 " .. will use default ddm=%s as (local) destination" % (activity, e.lfn, ddm))
                e.ddmendpoint = ddm
            elif e.ddmendpoint not in storages:  ## fspec.ddmendpoint is not in associated storages => assume it as final (non local) alternative destination
                self.logger.info("[prepare_destinations][%s]: Requested fspec.ddmendpoint=%s is not in the list of allowed (local) destinations"
                                 " .. will consider default ddm=%s for transfer and tag %s as alt. location" % (activity, e.ddmendpoint, ddm, e.ddmendpoint))
                e.ddmendpoint = ddm
                e.ddmendpoint_alt = e.ddmendpoint  ###  consider me later

        return files

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

        # path = protocol.get('path', '').rstrip('/')
        # if not (ddm.is_deterministic or (path and path.endswith('/rucio'))):
        if not ddm.is_deterministic:
            raise PilotException('resolve_surl(): Failed to construct SURL for non deterministic ddm=%s: '
                                 'NOT IMPLEMENTED' % fspec.ddmendpoint, code=ErrorCodes.NONDETERMINISTICDDM)

        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), self.get_path(fspec.scope, fspec.lfn))
        return {'surl': surl}

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
            pfn = fspec.surl or getattr(fspec, 'pfn', None) or os.path.join(kwargs.get('workdir', ''), fspec.lfn)
            if not os.path.isfile(pfn) or not os.access(pfn, os.R_OK):
                msg = "Error: output pfn file does not exist: %s" % pfn
                self.logger.error(msg)
                self.trace_report.update(clientState='MISSINGOUTPUTFILE', stateReason=msg)
                self.trace_report.send()
                raise PilotException(msg, code=ErrorCodes.MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")
            if not fspec.filesize:
                fspec.filesize = os.path.getsize(pfn)

            if not fspec.filesize:
                msg = 'output file has size zero: %s' % fspec.lfn
                self.logger.fatal(msg)
                raise PilotException(msg, code=ErrorCodes.ZEROFILESIZE, state="ZERO_FILE_SIZE")

            fspec.surl = pfn
            fspec.activity = activity
            if not fspec.checksum.get('adler32'):
                fspec.checksum['adler32'] = calculate_checksum(pfn)

        # prepare files (resolve protocol/transfer url)
        if getattr(copytool, 'require_protocols', True) and files:
            self.require_protocols(files, copytool, activity)

        if not copytool.is_valid_for_copy_out(files):
            self.logger.warning('Input is not valid for transfers using copytool=%s' % copytool)
            self.logger.debug('Input: %s' % files)
            raise PilotException('Invalid input for transfer operation')

        self.logger.info('ready to transfer (stage-out) files: %s' % files)

        if self.infosys:
            kwargs['copytools'] = self.infosys.queuedata.copytools

            # some copytools will need to know endpoint specifics (e.g. the space token) stored in ddmconf, add it
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()

        if not files:
            msg = 'nothing to stage-out - an internal Pilot error has occurred'
            self.logger.fatal(msg)
            raise PilotException(msg, code=errors.INTERNALPILOTPROBLEM)

        # add the trace report
        kwargs['trace_report'] = self.trace_report

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
