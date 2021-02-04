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
# - Alexey Anisenkov, anisyonk@cern.ch, 2018-2019

# refactored by Alexey Anisenkov

import os
import hashlib
import logging
import time

try:
    from functools import reduce  # Python 3
except Exception:
    pass

from pilot.info import infosys
from pilot.common.exception import PilotException, ErrorCodes, SizeTooLarge, NoLocalSpace, ReplicasNotFound
from pilot.util.auxiliary import show_memory_usage
from pilot.util.config import config
from pilot.util.filehandling import calculate_checksum, write_json
from pilot.util.math import convert_mb_to_b
from pilot.util.parameters import get_maximum_input_sizes
from pilot.util.workernode import get_local_disk_space
from pilot.util.timer import TimeoutException
from pilot.util.tracereport import TraceReport


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
    direct_remoteinput_allowed_schemas = ['root', 'https']
    # list of schemas to be used for direct acccess mode from LOCAL replicas
    direct_localinput_allowed_schemas = ['root', 'dcache', 'dcap', 'file', 'https']
    # list of allowed schemas to be used for transfers from REMOTE sites
    remoteinput_allowed_schemas = ['root', 'gsiftp', 'dcap', 'davs', 'srm', 'storm', 'https']

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

        try:
            if isinstance(acopytools, basestring):  # Python 2
                acopytools = {'default': [acopytools]} if acopytools else {}
        except Exception:
            if isinstance(acopytools, str):  # Python 3
                acopytools = {'default': [acopytools]} if acopytools else {}

        if isinstance(acopytools, (list, tuple)):
            acopytools = {'default': acopytools} if acopytools else {}

        self.acopytools = acopytools or {}

        if self.infosys.queuedata:
            self.set_acopytools()

        if not self.acopytools.get('default'):
            self.acopytools['default'] = self.get_default_copytools(default_copytools)

        if not self.acopytools:
            msg = 'failed to initilize StagingClient: no acopytools options found, acopytools=%s' % self.acopytools
            logger.error(msg)
            self.trace_report.update(clientState='BAD_COPYTOOL', stateReason=msg)
            self.trace_report.send()
            raise PilotException("failed to resolve acopytools settings")
        logger.info('configured copytools per activity: acopytools=%s' % self.acopytools)

        # get an initialized trace report (has to be updated for get/put if not defined before)
        self.trace_report = trace_report if trace_report else TraceReport(pq=os.environ.get('PILOT_SITENAME', ''))

    def set_acopytools(self):
        """
        Set the internal acopytools.

        :return:
        """
        if not self.acopytools:  # resolve from queuedata.acopytools using infosys
            self.acopytools = (self.infosys.queuedata.acopytools or {}).copy()
        if not self.acopytools:  # resolve from queuedata.copytools using infosys
            self.acopytools = dict(default=list((self.infosys.queuedata.copytools or {}).keys()))  # Python 2/3
            #self.acopytools = dict(default=(self.infosys.queuedata.copytools or {}).keys())  # Python 2

    @staticmethod
    def get_default_copytools(default_copytools):
        """
        Get the default copytools.

        :param default_copytools:
        :return: default copytools (string).
        """
        try:
            if isinstance(default_copytools, basestring):  # Python 2
                default_copytools = [default_copytools] if default_copytools else []
        except Exception:
            if isinstance(default_copytools, str):  # Python 3
                default_copytools = [default_copytools] if default_copytools else []
        return default_copytools

    @classmethod
    def get_preferred_replica(self, replicas, allowed_schemas):
        """
            Get preferred replica from the `replicas` list suitable for `allowed_schemas`
            :return: first matched replica or None if not found
        """

        for replica in replicas:
            pfn = replica.get('pfn')
            for schema in allowed_schemas:
                if pfn and (not schema or pfn.startswith('%s://' % schema)):
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
        try:
            if isinstance(activities, basestring):  # Python 2
                activities = [activities]
        except Exception:
            if isinstance(activities, str):  # Python 3
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

    @classmethod
    def sort_replicas(self, replicas, inputddms):
        """
        Sort input replicas: consider first affected replicas from inputddms
        :param replicas: Prioritized list of replicas [(pfn, dat)]
        :param inputddms: preferred list of ddmebdpoint
        :return: sorted `replicas`
        """

        if not inputddms:
            return replicas

        # group replicas by ddmendpoint to properly consider priority of inputddms
        ddmreplicas = {}
        for pfn, xdat in replicas:
            ddmreplicas.setdefault(xdat.get('rse'), []).append((pfn, xdat))

        # process LAN first (keep fspec.inputddms priorities)
        xreplicas = []
        for ddm in inputddms:
            xreplicas.extend(ddmreplicas.get(ddm) or [])

        for pfn, xdat in replicas:
            if (pfn, xdat) in xreplicas:
                continue
            xreplicas.append((pfn, xdat))

        return replicas

    def resolve_replicas(self, files, use_vp=False):
        """
        Populates filespec.replicas for each entry from `files` list

            fdat.replicas = [{'ddmendpoint':'ddmendpoint', 'pfn':'replica', 'domain':'domain value'}]

        :param files: list of `FileSpec` objects.
        :param use_vp: True for VP jobs (boolean).
        :return: `files`
        """

        logger = self.logger
        xfiles = []

        show_memory_usage()

        for fdat in files:
            ## skip fdat if need for further workflow (e.g. to properly handle OS ddms)
            xfiles.append(fdat)

        show_memory_usage()

        if not xfiles:  # no files for replica look-up
            return files

        # load replicas from Rucio
        from rucio.client import Client
        c = Client()

        show_memory_usage()

        location = self.detect_client_location()
        if not location:
            raise PilotException("Failed to get client location for Rucio", code=ErrorCodes.RUCIOLOCATIONFAILED)

        query = {
            'schemes': ['srm', 'root', 'davs', 'gsiftp', 'https', 'storm'],
            'dids': [dict(scope=e.scope, name=e.lfn) for e in xfiles],
        }
        query.update(sort='geoip', client_location=location)
        # reset the schemas for VP jobs
        if use_vp:
            query['schemes'] = ['root']
            query['rse_expression'] = 'istape=False\\type=SPECIAL'

        logger.info('calling rucio.list_replicas() with query=%s' % query)

        try:
            replicas = c.list_replicas(**query)
        except Exception as e:
            raise PilotException("Failed to get replicas from Rucio: %s" % e, code=ErrorCodes.RUCIOLISTREPLICASFAILED)

        show_memory_usage()

        replicas = list(replicas)
        logger.debug("replicas received from Rucio: %s" % replicas)

        files_lfn = dict(((e.scope, e.lfn), e) for e in xfiles)
        for replica in replicas:
            k = replica['scope'], replica['name']
            fdat = files_lfn.get(k)
            if not fdat:  # not requested replica
                continue

            # add the replicas to the fdat structure
            fdat = self.add_replicas(fdat, replica)

            # verify filesize and checksum values
            self.trace_report.update(validateStart=time.time())
            status = True
            if fdat.filesize != replica['bytes']:
                logger.warning("Filesize of input file=%s mismatched with value from Rucio replica: filesize=%s, replica.filesize=%s, fdat=%s"
                               % (fdat.lfn, fdat.filesize, replica['bytes'], fdat))
                status = False

            if not fdat.filesize:
                fdat.filesize = replica['bytes']
                logger.warning("Filesize value for input file=%s is not defined, assigning info from Rucio replica: filesize=%s" % (fdat.lfn, replica['bytes']))

            for ctype in ['adler32', 'md5']:
                if fdat.checksum.get(ctype) != replica[ctype] and replica[ctype]:
                    logger.warning("Checksum value of input file=%s mismatched with info got from Rucio replica: checksum=%s, replica.checksum=%s, fdat=%s"
                                   % (fdat.lfn, fdat.checksum, replica[ctype], fdat))
                    status = False

                if not fdat.checksum.get(ctype) and replica[ctype]:
                    fdat.checksum[ctype] = replica[ctype]

            if not status:
                logger.info("filesize and checksum verification done")
                self.trace_report.update(clientState="DONE")

        show_memory_usage()

        logger.info('Number of resolved replicas:\n' +
                    '\n'.join(["lfn=%s: replicas=%s, is_directaccess=%s"
                               % (f.lfn, len(f.replicas or []), f.is_directaccess(ensure_replica=False)) for f in files]))

        return files

    def add_replicas(self, fdat, replica):
        """
        Add the replicas to the fdat structure.

        :param fdat:
        :param replica:
        :return: updated fdat.
        """

        fdat.replicas = []  # reset replicas list

        # sort replicas by priority value
        try:
            sorted_replicas = sorted(replica.get('pfns', {}).iteritems(), key=lambda x: x[1]['priority'])  # Python 2
        except Exception:
            sorted_replicas = sorted(iter(list(replica.get('pfns', {}).items())),
                                     key=lambda x: x[1]['priority'])  # Python 3

        # prefer replicas from inputddms first
        xreplicas = self.sort_replicas(sorted_replicas, fdat.inputddms)

        for pfn, xdat in xreplicas:

            if xdat.get('type') != 'DISK':  # consider only DISK replicas
                continue

            rinfo = {'pfn': pfn, 'ddmendpoint': xdat.get('rse'), 'domain': xdat.get('domain')}

            ## (TEMPORARY?) consider fspec.inputddms as a primary source for local/lan source list definition
            ## backward compartible logic -- FIX ME LATER if NEED
            ## in case we should rely on domain value from Rucio, just remove the overwrite line below
            rinfo['domain'] = 'lan' if rinfo['ddmendpoint'] in fdat.inputddms else 'wan'

            if not fdat.allow_lan and rinfo['domain'] == 'lan':
                continue
            if not fdat.allow_wan and rinfo['domain'] == 'wan':
                continue

            fdat.replicas.append(rinfo)

        if not fdat.replicas:
            self.logger.warning('no replicas were selected (verify replica type, allow_lan/wan and domain values)')

        return fdat

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
            :return: list of processed `FileSpec` objects
        """

        self.trace_report.update(relativeStart=time.time(), transferStart=time.time())

        try:
            if isinstance(activity, basestring):  # Python 2
                activity = [activity]
        except Exception:
            if isinstance(activity, str):  # Python 3
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
                if config.Payload.executor_type.lower() == 'raythena':
                    fspec.status = 'no_transfer'

                try:
                    fspec.ddm_activity = filter(None, ['read_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'read_wan'])  # Python 2
                except Exception:
                    fspec.ddm_activity = [_f for _f in
                                          ['read_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'read_wan'] if
                                          _f]  # Python 3
            else:
                try:
                    fspec.ddm_activity = filter(None, ['write_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'write_wan'])  # Python 2
                except Exception:
                    fspec.ddm_activity = [_f for _f in
                                          ['write_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'write_wan']
                                          if _f]  # Python 3
        caught_errors = []

        for name in copytools:

            # get remain files that need to be transferred by copytool
            remain_files = [e for e in files if e.status not in ['remote_io', 'transferred', 'no_transfer']]

            if not remain_files:
                break

            try:
                if name not in self.copytool_modules:
                    raise PilotException('passed unknown copytool with name=%s .. skipped' % name,
                                         code=ErrorCodes.UNKNOWNCOPYTOOL)

                module = self.copytool_modules[name]['module_name']
                self.logger.info('trying to use copytool=%s for activity=%s' % (name, activity))
                copytool = __import__('pilot.copytool.%s' % module, globals(), locals(), [module], 0)  # Python 2/3
                self.trace_report.update(protocol=name)

            except PilotException as e:
                caught_errors.append(e)
                self.logger.debug('error: %s' % e)
                continue
            except Exception as e:
                self.logger.warning('failed to import copytool module=%s, error=%s' % (module, e))
                continue

            try:
                self.logger.debug('kwargs=%s' % str(kwargs))
                result = self.transfer_files(copytool, remain_files, activity, **kwargs)
                self.logger.debug('transfer_files() using copytool=%s completed with result=%s' % (copytool, str(result)))
                show_memory_usage()
                break
            except PilotException as e:
                self.logger.warning('failed to transfer_files() using copytool=%s .. skipped; error=%s' % (copytool, e))
                caught_errors.append(e)
            except TimeoutException as e:
                self.logger.warning('function timed out: %s' % e)
                caught_errors.append(e)
            except Exception as e:
                self.logger.warning('failed to transfer files using copytool=%s .. skipped; error=%s' % (copytool, e))
                caught_errors.append(e)
                import traceback
                self.logger.error(traceback.format_exc())

            if caught_errors and isinstance(caught_errors[-1], PilotException) and \
                    caught_errors[-1].get_error_code() == ErrorCodes.MISSINGOUTPUTFILE:
                raise caught_errors[-1]

        remain_files = [f for f in files if f.status not in ['remote_io', 'transferred', 'no_transfer']]

        if remain_files:  # failed or incomplete transfer
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
                code = ErrorCodes.STAGEINTIMEOUT if self.mode == 'stage-in' else ErrorCodes.STAGEOUTTIMEOUT  # is it stage-in/out?
                self.logger.warning('caught time-out exception: %s' % caught_errors[0])
            else:
                code = ErrorCodes.STAGEINFAILED if self.mode == 'stage-in' else ErrorCodes.STAGEOUTFAILED  # is it stage-in/out?
            details = str(caught_errors) + ":" + 'failed to transfer files using copytools=%s' % copytools
            self.logger.fatal(details)
            raise PilotException(details, code=code)

        return files

    def require_protocols(self, files, copytool, activity, local_dir=''):
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

        if local_dir:
            for fdat in files:
                if not local_dir.endswith('/'):
                    local_dir += '/'
                fdat.protocols = [{'endpoint': local_dir, 'flavour': '', 'id': 0, 'path': ''}]
        else:
            files = self.resolve_protocols(files)

        ddmconf = self.infosys.resolve_storage_data()

        for fspec in files:

            protocols = self.resolve_protocol(fspec, allowed_schemas)
            if not protocols and 'mv' not in self.infosys.queuedata.copytools:  # no protocols found
                error = 'Failed to resolve protocol for file=%s, allowed_schemas=%s, fspec=%s' % (fspec.lfn, allowed_schemas, fspec)
                self.logger.error("resolve_protocol: %s" % error)
                raise PilotException(error, code=ErrorCodes.NOSTORAGEPROTOCOL)

            # take first available protocol for copytool: FIX ME LATER if need (do iterate over all allowed protocols?)
            protocol = protocols[0]

            self.logger.info("Resolved protocol to be used for transfer: \'%s\': lfn=\'%s\'" % (protocol, fspec.lfn))

            resolve_surl = getattr(copytool, 'resolve_surl', None)
            if not callable(resolve_surl):
                resolve_surl = self.resolve_surl

            r = resolve_surl(fspec, protocol, ddmconf, local_dir=local_dir)  # pass ddmconf for possible custom look up at the level of copytool
            self.logger.debug('r=%s' % str(r))
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

    def resolve_replica(self, fspec, primary_schemas=None, allowed_schemas=None, domain=None):
        """
            Resolve input replica (matched by `domain` if need) first according to `primary_schemas`,
            if not found then look up within `allowed_schemas`
            Primary schemas ignore replica priority (used to resolve direct access replica, which could be not with top priority set)
            :param fspec: input `FileSpec` objects
            :param allowed_schemas: list of allowed schemas or any if None
            :return: dict(surl, ddmendpoint, pfn, domain) or None if replica not found
        """

        if not fspec.replicas:
            self.logger.warning('resolve_replica() received no fspec.replicas')
            return

        allowed_schemas = allowed_schemas or [None]
        primary_replica, replica = None, None

        # group by ddmendpoint to look up related surl/srm value
        replicas = {}

        for rinfo in fspec.replicas:

            replicas.setdefault(rinfo['ddmendpoint'], []).append(rinfo)

            if rinfo['domain'] != domain:
                continue
            if primary_schemas and not primary_replica:  # look up primary schemas if requested
                primary_replica = self.get_preferred_replica([rinfo], primary_schemas)
            if not replica:
                replica = self.get_preferred_replica([rinfo], allowed_schemas)

            if replica and primary_replica:
                break

        replica = primary_replica or replica

        if not replica:  # replica not found
            schemas = 'any' if not allowed_schemas[0] else ','.join(allowed_schemas)
            pschemas = 'any' if primary_schemas and not primary_schemas[0] else ','.join(primary_schemas or [])

            error = 'Failed to find replica for file=%s, domain=%s, allowed_schemas=%s, pschemas=%s, fspec=%s' % (fspec.lfn, domain, schemas, pschemas, fspec)
            self.logger.info("resolve_replica: %s" % error)
            return

        # prefer SRM protocol for surl -- to be verified, can it be deprecated?
        rse_replicas = replicas.get(replica['ddmendpoint'], [])
        surl = self.get_preferred_replica(rse_replicas, ['srm']) or rse_replicas[0]
        self.logger.info("[stage-in] surl (srm replica) from Rucio: pfn=%s, ddmendpoint=%s" % (surl['pfn'], surl['ddmendpoint']))

        return {'surl': surl['pfn'], 'ddmendpoint': replica['ddmendpoint'], 'pfn': replica['pfn'], 'domain': replica['domain']}

    def get_direct_access_variables(self, job):
        """
        Return the direct access settings for the PQ.

        :param job: job object.
        :return: allow_direct_access (bool), direct_access_type (string).
        """

        allow_direct_access, direct_access_type = False, ''
        if self.infosys.queuedata:  # infosys is initialized
            allow_direct_access = self.infosys.queuedata.direct_access_lan or self.infosys.queuedata.direct_access_wan
            if self.infosys.queuedata.direct_access_lan:
                direct_access_type = 'LAN'
            if self.infosys.queuedata.direct_access_wan:
                direct_access_type = 'WAN'
        else:
            self.logger.info('infosys.queuedata is not initialized: direct access mode will be DISABLED by default')

        if job and not job.is_analysis() and job.transfertype != 'direct':  # task forbids direct access
            allow_direct_access = False
            self.logger.info('switched off direct access mode for production job since transfertype=%s' % job.transfertype)

        return allow_direct_access, direct_access_type

    #def set_accessmodes_for_direct_access(self, files, direct_access_type):  ## TO BE DEPRECATED (anisyonk)
    #    """
    #    Update the FileSpec accessmodes for direct access and sort the files to get candidates for remote_io coming
    #    first in order to exclude them from checking of available space for stage-in.
    #
    #    :param files: FileSpec objects.
    #    :param direct_access_type: type of direct access (LAN or WAN) (string).
    #    :return:
    #    """
    #
    #    # sort the files
    #    files = sorted(files, key=lambda x: x.is_directaccess(ensure_replica=False), reverse=True)
    #
    #    # populate allowremoteinputs for each FileSpec object
    #    for fdata in files:
    #        is_directaccess = fdata.is_directaccess(ensure_replica=False)
    #        if is_directaccess and direct_access_type == 'WAN':  ## is it the same for ES workflow ?? -- test and verify/FIXME LATER
    #            fdata.allowremoteinputs = True
    #        self.logger.info("check direct access for lfn=%s: allow_direct_access=true, fdata.is_directaccess()=%s =>"
    #                         " is_directaccess=%s, allowremoteinputs=%s" % (fdata.lfn,
    #                                                                        fdata.is_directaccess(ensure_replica=False),
    #                                                                        is_directaccess, fdata.allowremoteinputs))
    #        # must update accessmode for user jobs (it is only set already for production jobs)
    #        if fdata.accessmode != 'direct' and is_directaccess and fdata.accessmode != 'copy':
    #            fdata.accessmode = 'direct'
    #
    #        # reset accessmode if direct access is not to be used
    #        if fdata.accessmode == 'direct' and not is_directaccess:
    #            fdata.accessmode = ''
    #
    #        self.logger.info('accessmode for LFN=%s: %s (is_directaccess=%s)' % (fdata.lfn, fdata.accessmode, is_directaccess))

    def transfer_files(self, copytool, files, activity=None, **kwargs):  # noqa: C901
        """
        Automatically stage in files using the selected copy tool module.

        :param copytool: copytool module
        :param files: list of `FileSpec` objects
        :param kwargs: extra kwargs to be passed to copytool transfer handler

        :return: list of processed `FileSpec` objects
        :raise: PilotException in case of controlled error
        """

        if getattr(copytool, 'require_replicas', False) and files:
            if files[0].replicas is None:  # look up replicas only once
                files = self.resolve_replicas(files, use_vp=kwargs['use_vp'])

            allowed_schemas = getattr(copytool, 'allowed_schemas', None)

            if self.infosys and self.infosys.queuedata:
                copytool_name = copytool.__name__.rsplit('.', 1)[-1]
                allowed_schemas = self.infosys.queuedata.resolve_allowed_schemas(activity, copytool_name) or allowed_schemas

            # overwrite allowed_schemas for VP jobs
            if kwargs['use_vp']:
                allowed_schemas = ['root']
                self.logger.debug('overwrote allowed_schemas for VP job: %s' % str(allowed_schemas))

            for fspec in files:
                resolve_replica = getattr(copytool, 'resolve_replica', None)
                resolve_replica = self.resolve_replica if not callable(resolve_replica) else resolve_replica

                replica = None

                # process direct access logic  ## TODO move to upper level, should not be dependent on copytool (anisyonk)
                # check local replicas first
                if fspec.allow_lan:
                    # prepare schemas which will be used to look up first the replicas allowed for direct access mode
                    primary_schemas = (self.direct_localinput_allowed_schemas if fspec.direct_access_lan and
                                       fspec.is_directaccess(ensure_replica=False) else None)
                    replica = resolve_replica(fspec, primary_schemas, allowed_schemas, domain='lan')
                else:
                    self.logger.info("[stage-in] LAN access is DISABLED for lfn=%s (fspec.allow_lan=%s)" % (fspec.lfn, fspec.allow_lan))

                if not replica and fspec.allow_lan:
                    self.logger.info("[stage-in] No LAN replica found for lfn=%s, primary_schemas=%s, allowed_schemas=%s" %
                                     (fspec.lfn, primary_schemas, allowed_schemas))

                # check remote replicas
                if not replica and fspec.allow_wan:
                    # prepare schemas which will be used to look up first the replicas allowed for direct access mode
                    primary_schemas = (self.direct_remoteinput_allowed_schemas if fspec.direct_access_wan and
                                       fspec.is_directaccess(ensure_replica=False) else None)
                    xschemas = self.remoteinput_allowed_schemas
                    allowed_schemas = [e for e in allowed_schemas if e in xschemas] if allowed_schemas else xschemas
                    replica = resolve_replica(fspec, primary_schemas, allowed_schemas, domain='wan')

                if not replica and fspec.allow_wan:
                    self.logger.info("[stage-in] No WAN replica found for lfn=%s, primary_schemas=%s, allowed_schemas=%s" %
                                     (fspec.lfn, primary_schemas, allowed_schemas))
                if not replica:
                    raise ReplicasNotFound('No replica found for lfn=%s (allow_lan=%s, allow_wan=%s)' % (fspec.lfn, fspec.allow_lan, fspec.allow_wan))

                if replica.get('pfn'):
                    fspec.turl = replica['pfn']
                if replica.get('surl'):
                    fspec.surl = replica['surl']  # TO BE CLARIFIED if it's still used and need
                if replica.get('ddmendpoint'):
                    fspec.ddmendpoint = replica['ddmendpoint']
                if replica.get('domain'):
                    fspec.domain = replica['domain']

                self.logger.info("[stage-in] found replica to be used for lfn=%s: ddmendpoint=%s, pfn=%s" %
                                 (fspec.lfn, fspec.ddmendpoint, fspec.turl))

        # prepare files (resolve protocol/transfer url)
        if getattr(copytool, 'require_input_protocols', False) and files:
            self.require_protocols(files, copytool, activity, local_dir=kwargs['input_dir'])

        # mark direct access files with status=remote_io
        self.set_status_for_direct_access(files, kwargs.get('workdir', ''))

        # get remain files that need to be transferred by copytool
        remain_files = [e for e in files if e.status not in ['direct', 'remote_io', 'transferred', 'no_transfer']]

        if not remain_files:
            return files

        if not copytool.is_valid_for_copy_in(remain_files):
            msg = 'input is not valid for transfers using copytool=%s' % copytool
            self.logger.warning(msg)
            self.logger.debug('input: %s' % remain_files)
            self.trace_report.update(clientState='NO_REPLICA', stateReason=msg)
            self.trace_report.send()
            raise PilotException('invalid input data for transfer operation')

        if self.infosys:
            if self.infosys.queuedata:
                kwargs['copytools'] = self.infosys.queuedata.copytools
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()
        kwargs['activity'] = activity

        # verify file sizes and available space for stage-in
        if self.infosys.queuedata.maxinputsize != -1:
            self.check_availablespace(remain_files)
        else:
            self.logger.info('skipping input file size check since maxinputsize=-1')
        show_memory_usage()

        # add the trace report
        kwargs['trace_report'] = self.trace_report
        self.logger.info('ready to transfer (stage-in) files: %s' % remain_files)

        # use bulk downloads if necessary
        # if kwargs['use_bulk_transfer']
        # return copytool.copy_in_bulk(remain_files, **kwargs)
        return copytool.copy_in(remain_files, **kwargs)

    def set_status_for_direct_access(self, files, workdir):
        """
        Update the FileSpec status with 'remote_io' for direct access mode.
        Should be called only once since the function sends traces

        :param files: list of FileSpec objects.
        :param workdir: work directory (string).
        :return: None
        """

        for fspec in files:
            direct_lan = (fspec.domain == 'lan' and fspec.direct_access_lan and
                          fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=self.direct_localinput_allowed_schemas))
            direct_wan = (fspec.domain == 'wan' and fspec.direct_access_wan and
                          fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=self.remoteinput_allowed_schemas))

            if not direct_lan and not direct_wan:
                self.logger.debug('direct lan/wan transfer will not be used for lfn=%s' % fspec.lfn)
            self.logger.debug('lfn=%s, direct_lan=%s, direct_wan=%s, direct_access_lan=%s, direct_access_wan=%s, '
                              'direct_localinput_allowed_schemas=%s, remoteinput_allowed_schemas=%s' %
                              (fspec.lfn, direct_lan, direct_wan, fspec.direct_access_lan, fspec.direct_access_wan,
                               str(self.direct_localinput_allowed_schemas), str(self.remoteinput_allowed_schemas)))

            if direct_lan or direct_wan:
                fspec.status_code = 0
                fspec.status = 'remote_io'

                self.logger.info('stage-in: direct access (remote i/o) will be used for lfn=%s (direct_lan=%s, direct_wan=%s), turl=%s' %
                                 (fspec.lfn, direct_lan, direct_wan, fspec.turl))

                # send trace
                localsite = os.environ.get('RUCIO_LOCAL_SITE_ID')
                localsite = localsite or fspec.ddmendpoint
                self.trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
                self.trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
                self.trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
                self.trace_report.update(url=fspec.turl, clientState='FOUND_ROOT', stateReason='direct_access')

                # do not send the trace report at this point if remote file verification is to be done
                # note also that we can't verify the files at this point since root will not be available from inside
                # the rucio container
                if config.Pilot.remotefileverification_log:
                    # store the trace report for later use (the trace report class inherits from dict, so just write it as JSON)
                    # outside of the container, it will be available in the normal work dir
                    # use the normal work dir if we are not in a container
                    _workdir = workdir if os.path.exists(workdir) else '.'
                    path = os.path.join(_workdir, config.Pilot.base_trace_report)
                    if not os.path.exists(_workdir):
                        path = os.path.join('/srv', config.Pilot.base_trace_report)
                    if not os.path.exists(path):
                        self.logger.debug('writing base trace report to: %s' % path)
                        write_json(path, self.trace_report)
                else:
                    self.trace_report.send()

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

        if not self.infosys.queuedata:  # infosys is not initialized: not able to fix destination if need, nothing to do
            return files

        try:
            if isinstance(activities, (str, unicode)):  # Python 2
                activities = [activities]
        except Exception:
            if isinstance(activities, str):  # Python 3
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
            if 'mv' in self.infosys.queuedata.copytools:
                return files
            else:
                raise PilotException("Failed to resolve destination: no associated storages defined for activity=%s (%s)"
                                     % (activity, ','.join(activities)), code=ErrorCodes.NOSTORAGE, state='NO_ASTORAGES_DEFINED')

        # take the fist choice for now, extend the logic later if need
        ddm = storages[0]

        self.logger.info("[prepare_destinations][%s]: allowed (local) destinations: %s" % (activity, storages))
        self.logger.info("[prepare_destinations][%s]: resolved default destination ddm=%s" % (activity, ddm))

        for e in files:
            if not e.ddmendpoint:  # no preferences => use default destination
                self.logger.info("[prepare_destinations][%s]: fspec.ddmendpoint is not set for lfn=%s"
                                 " .. will use default ddm=%s as (local) destination" % (activity, e.lfn, ddm))
                e.ddmendpoint = ddm
            elif e.ddmendpoint not in storages:  # fspec.ddmendpoint is not in associated storages => assume it as final (non local) alternative destination
                self.logger.info("[prepare_destinations][%s]: Requested fspec.ddmendpoint=%s is not in the list of allowed (local) destinations"
                                 " .. will consider default ddm=%s for transfer and tag %s as alt. location" % (activity, e.ddmendpoint, ddm, e.ddmendpoint))
                e.ddmendpoint = ddm
                e.ddmendpoint_alt = e.ddmendpoint  # consider me later

        return files

    @classmethod
    def get_path(self, scope, lfn, prefix='rucio'):
        """
            Construct a partial Rucio PFN using the scope and the LFN
        """

        # <prefix=rucio>/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>

        s = '%s:%s' % (scope, lfn)
        hash_hex = hashlib.md5(s.encode('utf-8')).hexdigest()  # Python 2/3

        #paths = [prefix] + scope.split('.') + [hash_hex[0:2], hash_hex[2:4], lfn]
        # exclude prefix from the path: this should be properly considered in protocol/AGIS for today
        paths = scope.split('.') + [hash_hex[0:2], hash_hex[2:4], lfn]
        try:
            paths = filter(None, paths)  # remove empty parts to avoid double /-chars, Python 2
        except Exception:
            paths = [_f for _f in paths if _f]  # remove empty parts to avoid double /-chars, Python 3

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

        local_dir = kwargs.get('local_dir', '')
        if not local_dir:
            # consider only deterministic sites (output destination) - unless local input/output
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

            if not fspec.ddmendpoint:  # ensure that output destination is properly set
                if 'mv' not in self.infosys.queuedata.copytools:
                    msg = 'No output RSE defined for file=%s' % fspec.lfn
                    self.logger.error(msg)
                    raise PilotException(msg, code=ErrorCodes.NOSTORAGE, state='NO_OUTPUTSTORAGE_DEFINED')

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
            try:
                output_dir = kwargs['output_dir']
            except Exception:
                output_dir = ""
            self.require_protocols(files, copytool, activity, local_dir=output_dir)

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
            raise PilotException(msg, code=ErrorCodes.INTERNALPILOTPROBLEM)

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
