#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2021

# based on Harvester k8s_utils

import os

from kubernetes import client, config
# from kubernetes.client.rest import ApiException


class K8sClient(object):

    def __init__(self, namespace, config_file=None):
        """
        Init.
        """

        if not os.path.isfile(config_file):
            raise RuntimeError('Cannot find k8s config file: {0}'.format(config_file))

        config.load_kube_config(config_file=config_file)
        self.namespace = namespace if namespace else 'default'
        self.core_v1 = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.delete_v1 = client.V1DeleteOptions(propagation_policy='Background')

    def create_ssh_keys_secret(self, work_spec):
        return True

    def create_dask_head(self, work_spec, panda_queue, evaluation_image, pilot_image, cert, dfs_claim_name,
                         cpu_adjust_ratio=100, memory_adjust_ratio=100, max_time=None):
        """

        """

        #worker_id = str(work_spec.workerID)
        #tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
        #                                 method_name='create_dask_head')

        if not self.create_configmap_dask(work_spec, panda_queue):
            return False

        if not self.create_host_discovery_configmap(work_spec):
            return False

    def create_dask_formation(self, work_spec, panda_queue, evaluation_image, pilot_image,
                              worker_command, cert, dfs_claim_name, cpu_adjust_ratio=100, memory_adjust_ratio=100,
                              max_time=None):

        rsp = self.create_ssh_keys_secret(work_spec)
        if not rsp:
            return rsp

        rsp = self.create_dask_head(work_spec, panda_queue, evaluation_image, pilot_image, cert, dfs_claim_name,
                                    cpu_adjust_ratio, memory_adjust_ratio, max_time)
        if not rsp:
            return rsp

        rsp = self.create_dask_workers(work_spec, evaluation_image, worker_command, dfs_claim_name,
                                       cpu_adjust_ratio, memory_adjust_ratio, max_time)

        if not rsp:
            return rsp

        return True
