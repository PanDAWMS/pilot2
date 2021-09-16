#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

#try:
#    # import dask
#    import dask_kubernetes
##except ModuleNotFoundError:  # Python 3
#except Exception:
#    pass

#from pilot.common.exception import NotDefined, NotSameLength, UnknownException
#from pilot.util.container import execute
#from pilot.util.filehandling import establish_logging, write_file

#import os
#import re
#from time import sleep

import logging
logger = logging.getLogger(__name__)


class DaskSubmitter(object):
    """
    Dask submitter interface class.
    """

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs:
        """

        pass

    def install(self, job_definition):
        """
        Install the pods for the dask scheduler and workers, and Pilot X

        Note: Pilot X is currently a simplified PanDA Pilot, but is likely to be absorbed into the main
        PanDA Pilot code base as a special workflow for Dask on Kubernetes resources.

        The install function will start by installing the Pilot X pod on the dask cluster. When it starts running,
        Pilot X will wait for a job definition to appear on the shared file system. It will then proceed staging any
        input files.

        In the meantime, this function (who also knows about the job definition) will asynchronously install the dask
        scheduler and all required workers

        :param job_definition: job definition dictionary.
        :return: True for successfully installed pods (Boolean).
        """

        # install Pilot X pod
        status = self.install_pilotx_pod()
        if not status:
            return status

        # copy bundle (job definition etc)

        # copy job definition to shared directory
        # (copy to Pilot X pod which has the shared directory mounted)
        status = self.copy_job_definition(job_definition)
        if not status:
            return status

        # install dask scheduler
        status = self.install_dask_scheduler()
        if not status:
            return status

        # install dask worker(s)
        status = self.install_dask_workers(job_definition)
        if not status:
            return status

        return status

    def uninstall(self):
        """
        Uninstall all pods.
        """

        # uninstall all pods
        # ..

        pass

    def install_pilotx_pod(self):
        """

        """

        status = True

        return status

    def copy_job_definition(self, job_definition):
        """

        """

        status = True

        return status

    def install_dask_scheduler(self):
        """

        """

        status = True

        return status

    def install_dask_workers(self, job_definition):
        """

        """

        status = True

        # get number of workers from job definition
        # issue all install commands at once, then wait for pod status to be 'running'

        return status
