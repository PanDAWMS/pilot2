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

# refactored by Alexey Anisenkov
import logging

from pilot.info import infosys
from pilot.common.exception import PilotException  #, ErrorCodes


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
                        'lsm': {'module_name': 'lsm'}
                        }

    direct_remoteinput_allowed_schemas = ['root']
    remoteinput_allowed_schemas = ['root', 'gsiftp', 'dcap', 'davs', 'srm']

    def __init__(self, infosys_instance=None, acopytools=None, logger=None, default_copytools='rucio'):
        """
            :param acopytools: dict of copytool names per activity to be used for transfers. If not specified will be automatically resolved via infosys.
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
            acopytools = [acopytools] if acopytools else []

        self.acopytools = acopytools
        if not self.acopytools:  # try to get associated copytools from queuedata
            self.acopytools = (self.infosys.queuedata.acopytools or {}).copy()
        if not self.acopytools:  ## resolve from queuedata.copytools
            self.acopytools = dict(default=(self.infosys.queuedata.copytools or {}).keys())

        if not self.acopytools:
            logger.error('Failed to initilize StagingClient: no acopytools options found, acopytools=' % self.acopytools)
            raise PilotException("Failed to resolve acopytools settings")

        if not self.acopytools.get('default'):
            if isinstance(default_copytools, basestring):
                default_copytools = [default_copytools] if default_copytools else []
            self.acopytools['default'] = default_copytools

        logger.info('Configured copytools per activity: acopytools=%s' % self.acopytools)

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

        bquery = {'schemes': ['srm', 'root', 'davs', 'gsiftp'],
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
            except TypeError, e:
                if query == bquery:
                    raise
                logger.warning("Detected outdated Rucio list_replicas(), cannot do geoip-sorting: %s .. fallback to old list_replicas() call" % e)
                replicas = c.list_replicas(**bquery)

        except Exception, e:
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
        except Exception, e:
            #self.log('socket() failed to lookup local IP')
            print 'socket() failed to lookup local IP: %s' % e

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

    def transfer(self, files, activity='default', **kwargs):
        """
            Automatically stage passed files using copy tools related to given `activity`
            :param files: list of `FileSpec` objects
            :param activity: activity name used to determine appropriate copytool
            :param kwargs: extra kwargs to be passed to copytool transfer handler
            :raise: PilotException in case of controlled error
            :return: output of copytool trasfers (to be clarified)
        """

        copytools = self.acopytools.get(activity) or self.acopytools.get('default')

        if not copytools:
            raise PilotException('Failed to resolve copytool by activity=%s, acopytools=%s' % (activity, self.acopytools))

        result, errors = None, []

        for name in copytools:

            try:
                if name not in self.copytool_modules:
                    raise PilotException('passed unknow copytool with name=%s .. skipped' % name)
                module = self.copytool_modules[name]['module_name']
                self.logger.info('Trying to use copytool=%s for activity=%s' % (name, activity))
                copytool = __import__('pilot.copytool.%s' % module, globals(), locals(), [module], -1)
            except PilotException, e:
                errors.append(e)
                continue
            except Exception, e:
                self.logger.warning('Failed to import copytool module=%s, error=%s' % (module, e))
                self.logger.debug('Error: %s' % e)
                continue
            try:
                result = self.transfer_files(copytool, files, **kwargs)
            except PilotException, e:
                errors.append(e)
            except Exception, e:
                self.logger.warning('Failed to transfer files using copytool=%s .. skipped; error=%s' % (copytool, e))

            if result:
                break

        if not result:
            raise PilotException('Failed to transfer files using copytools=%s, erros=%s' % (copytools, errors))

        return result


class StageInClient(StagingClient):

    def transfer_files(self, copytool, files, **kwargs):
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

        if not copytool.is_valid_for_copy_in(files):
            self.logger.warning('Input is not valid for transfers using copytool=%s' % copytool)
            self.logger.debug('Input: %s' % files)
            raise PilotException('Invalid input for transfer operation')

        return copytool.copy_in(files, **kwargs)


class StageOutClient(StagingClient):

    def transfer_files(self, copytool, files):
        """
            Automatically stage out files using the selected copy tool module.

            :param copytool: copytool module
            :param files: list of `FileSpec` objects

            :return: the output of the copytool transfer operation
            :raise: PilotException in case of controlled error
        """

        if not copytool.is_valid_for_copy_out(files):
            self.logger.warning('Input is not valid for transfers using copytool=%s' % copytool)
            self.logger.debug('Input: %s' % files)
            raise PilotException('Invalid input for transfer operation')

        return copytool.copy_out(files)

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
