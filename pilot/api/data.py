#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import os
import logging
import sys

from pilot.control import data


def setup_logging():
    """

    :return:
    """

    # Establish logging
    console = logging.StreamHandler(sys.stdout)
    logging.basicConfig(filename='transfer.txt', level=logging.INFO,
                        format='%(asctime)s | %(levelname)-8s | %(message)s')
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s'))
    logging.getLogger('').addHandler(console)


def shutdown_logging():
    """

    :return:
    """
    logging.shutdown()


class StageInClient(object):

    def __init__(self, site=None, copytool=None):
        super(StageInClient, self).__init__()

        self.copytool_name = copytool
        if self.copytool_name is None:
            self.copytool_name = 'rucio'

        # Check validity of specified site - should be refactored into VO-agnostic setup
        self.site = os.environ.get('VO_ATLAS_AGIS_SITE', site)
        if self.site is None and self.copytool_name == 'rucio':
            raise Exception('VO_ATLAS_AGIS_SITE not available, must set StageInClient(site=...) parameter')

        # Retrieve location information
        # will need this later -- don't spam AGIS for now
        # from pilot.util import information
        # self.args = collections.namedtuple('args', ['location'])
        # information.set_location(self.args, site=self.site)

    def transfer(self, files):
        """
        Automatically stage in files using rucio.

        :param files: List of dictionaries containing the file information
                      for the rucio copytool, this must contain DID and destination directory.
                      [{scope, name, destination
                      for other copytools, the dictionary must contain
                      [{name, source, destination
        :return: Annotated files -- List of dictionaries with additional variables.
                 [{..., errno, errmsg, status
        """

        # setup logging
        setup_logging()

        all_files_ok = False
        for f in files:
            if self.copytool_name == 'rucio':
                if all(key in f for key in ('scope', 'name', 'destination')):
                    all_files_ok = True
            else:
                if all(key in f for key in ('name', 'source', 'destination')):
                    all_files_ok = True

        if all_files_ok:
            copytool = __import__('pilot.copytool.%s' % self.copytool_name, globals(), locals(),
                                  [self.copytool_name], -1)
            copytool.copy_in(files)
        else:
            if self.copytool_name == 'rucio':
                raise Exception('Files dictionary does not conform to: scope, name, destination')
            else:
                raise Exception('Files dictionary does not conform to: name, source, destination')

        # shutdown logging
        shutdown_logging()


class StageOutClient(object):

    def __init__(self, site=None, copytool=None):
        super(StageOutClient, self).__init__()

        self.copytool_name = copytool
        if self.copytool_name is None:
            self.copytool_name = 'rucio'

        # Check validity of specified site - should be refactored into VO-agnostic setup
        self.site = os.environ.get('VO_ATLAS_AGIS_SITE', site)
        if self.site is None and self.copytool_name == 'rucio':
            raise Exception('VO_ATLAS_AGIS_SITE not available, must set StageOutClient(site=...) parameter')

        # Retrieve location information
        # will need this later -- don't spam AGIS for now
        # from pilot.util import information
        # self.args = collections.namedtuple('args', ['location'])
        # information.set_location(self.args, site=self.site)

    def transfer(self, files):
        """
        Automatically stage out files using rucio.

        :param files: List of dictionaries containing the target scope, the path to the file, and destination RSE.
                      [{scope, file, rse
                      Additional variables that can be used:
                        lifetime                        of the file in seconds
                        no_register                     setting this to True will not register the file to Rucio (CAREFUL!)
                        guid                            manually set guid of file in either string format
                                                         XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX or
                                                         XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
                        attach: {scope, name            automatically attach this file to the given dataset
                        summary                         setting this to True will write a rucio_upload.json with used PFNs
        :return: Annotated files -- List of dictionaries with additional variables.
                 [{..., errno, errmsg, status
        """

        # setup logging
        setup_logging()

        all_files_ok = False
        for f in files:
            if self.copytool_name == 'rucio':
                if all(key in f for key in ('scope', 'file', 'rse')):
                    all_files_ok = True
            else:
                if all(key in f for key in ('name', 'source', 'destination')):
                    all_files_ok = True

        if all_files_ok:
            if self.copytool_name == 'rucio':
                return data.stage_out_auto(self.site, files)
            else:
                copytool = __import__('pilot.copytool.%s' % self.copytool_name, globals(), locals(),
                      [self.copytool_name], -1)
                copytool.copy_out(files)

        else:
            if self.copytool_name == 'rucio':
                raise Exception('Files dictionary does not conform to: scope, file, rse')
            else:
                raise Exception('Files dictionary does not conform to: name, source, destination')

        # shutdown logging
        shutdown_logging()


class StageInClientAsync(object):

    def __init__(self, site):
        raise NotImplementedError

    def queue(self, files):
        raise NotImplementedError

    def is_transferring(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def finish(self):
        raise NotImplementedError

    def status(self):
        raise NotImplementedError


class StageOutClientAsync(object):

    def __init__(self, site):
        raise NotImplementedError

    def queue(self, files):
        raise NotImplementedError

    def is_transferring(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def finish(self):
        raise NotImplementedError

    def status(self):
        raise NotImplementedError
