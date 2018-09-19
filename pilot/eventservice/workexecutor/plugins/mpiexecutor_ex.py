#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018


import json
import os
import threading
import time
import Queue
import sys
import traceback
import uuid
from mpi4py import MPI

from pilot.eventservice.communicationmanager.communicationmanager import CommunicationManager
from pilot.eventservice.esprocess.esprocess import ESProcess
from .baseexecutor import BaseExecutor

import logging

"""
Manage to run EventService with MPI
"""


class MPIMessage(object):
    class MessageType(object):
        Request = 'request'
        Response = 'response'

    class MessageName(object):
        RequestJob = 'Request_Job'
        RequestEvents = 'Request_Events'
        OutputFile = 'Output_File'

    def __init__(self, attrs=None):
        if not attrs:
            attrs = {}
        if not isinstance(attrs, dict):
            attrs = json.loads(attrs)

        def_attrs = {'message_id': uuid.uuid1(),
                     'message_type': MPIMessage.MessageType.Request,
                     'message_name': None,
                     'from_rank': None,
                     'to_rank': None,
                     'content': None,
                     'wait_for_response': False,
                     'response': None}

        for key in def_attrs:
            if key not in attrs:
                attrs[key] = def_attrs[key]

        for key in attrs:
            setattr(self, key, attrs[key])

    def to_json(self):
        return json.dumps(self.__dict__)

    def generate_response(self):
        res_attrs = {}
        for key in self.__dict__:
            res_attrs[key] = self.__dict__[key]
        res_attrs['message_type'] = MPIMessage.MessageType.Response
        res_attrs['from_rank'] = self.to_rank
        res_attrs['to_rank'] = self.from_rank
        res_attrs['content'] = None
        res_attrs['wait_for_response'] = False
        return res_attrs


class MPIReceiver(threading.Thread):
    def __init__(self):
        super(MPIReceiver, self).__init__()

        comm = MPI.COMM_WORLD
        self.comm = comm
        self.stat = MPI.Status()
        self.receiver_queue = Queue.Queue()
        self.exit = threading.Event()

    def stop(self):
        self.exit.set()

    def get(self, block=True, timeout=None):
        return self.receiver_queue.get(block, timeout)

    def run(self):
        while (not self.exit.isSet()):
            has_req = False
            if self.comm.Iprobe(source=MPI.ANY_SOURCE, status=self.stat):
                data = self.comm.recv(source=self.stat.Get_source())
                req = MPIMessage(data)
                self.receiver_queue.put(req)
                has_req = True
            if not has_req:
                time.sleep(0.00001)


class MPIDeliver(threading.Thread):
    def __init__(self):
        super(MPIDeliver, self).__init__()

        comm = MPI.COMM_WORLD
        self.comm = comm
        self.stat = MPI.Status()
        self.deliver_queue = Queue.Queue()
        self.exit = threading.Event()

    def stop(self):
        self.exit.set()

    def put(self, req):
        self.deliver_queue.put(req)

    def run(self):
        while (not self.exit.isSet()):
            if not self.deliver_queue.empty():
                req = self.deliver_queue.get()
                self.comm.send(req.to_json(), dest=req.to_rank)
            else:
                time.sleep(0.00001)


class MPIService(threading.Thread):
    def __init__(self):
        super(MPIService, self).__init__()

        self.receiver_queue = Queue.Queue()
        self.deliver_queue = Queue.Queue()
        self.receiver = MPIReceiver()
        self.deliver = MPIDeliver()
        self.exit = threading.Event()
        self.req_resps = {}

    def stop(self):
        self.exit.set()

    def put(self, req):
        self.deliver_queue.put(req)

    def get(self, block=True, timeout=None):
        return self.receiver_queue.get(block, timeout)

    def get_response(self, req):
        if req.message_id in self.req_resps:
            while (self.req_resps[req.message_id].response is None):
                time.sleep(0.1)
            req = self.req_resps[req.message_id].response
            del self.req_resps[req.message_id]
            return req
        else:
            logging.error("The request is not delivered or its response is already fetched by client for request: %s" % req)
        return None

    def run(self):
        while not self.is_stop():
            req = self.deliver_queue.get(block=False)
            if req:
                if req.message_type == MPIMessage.MessageType.Request:
                    if req.wait_for_response:
                        self.req_resps[req.message_id] = req
                else:
                    if req.message_id in self.req_resps:
                        del self.req_resps[req.message_id]
                    else:
                        logging.warn("No corresponding request for response: %s" % req)
                self.deliver.put(req)
            req = self.receiver.get(block=False)
            if req:
                if req.message_type == MPIMessage.MessageType.Request:
                    if req.wait_for_response:
                        self.req_resps[req.message_id] = req
                    self.self.receiver_queue.put(req)
                else:
                    if req.message_id in self.req_resps:
                        self.req_resps[req.message_id].response = req
                    else:
                        logging.warn("No corresponding request for response: %s" % req)


class MPIWorker(threading.Thread):
    def __init__(self, work_dir, rank, default_event_ranges=1):
        super(MPIWorker, self).__init__()
        self.work_dir = work_dir
        self.rank = rank
        self.mpiservice = MPIService()

        self.default_event_ranges = default_event_ranges
        self.events = []
        self.output_messages = []

        self.__stop = threading.Event()

    def __del__(self):
        self.stop()
        if self.communication_manager:
            self.communication_manager.stop()

    def stop(self):
        if not self.is_stop():
            self.__stop.set()

    def is_stop(self):
        return self.__stop.isSet()

    def get_payload(self):
        """
        Get payload to execute.

        :returns: dict {'payload': <cmd string>, 'output_file': <filename or without it>, 'error_file': <filename or without it>}
        """
        req_attrs = {'message_type': MPIMessage.MessageType.Request,
                     'message_name': MPIMessage.MessageName.RequestJob,
                     'from_rank': self.rank,
                     'to_rank': 0,
                     'content': None,
                     'wait_for_response': True}
        req = MPIMessage(req_attrs)
        self.mpiservice.put(req)
        return self.mpiservice.get_response(req)

    def request_event_ranges(self):
        """
        Get event ranges.

        :returns: dict of event ranges.
                  None if no available events.
        """
        req_attrs = {'message_type': MPIMessage.MessageType.Request,
                     'message_name': MPIMessage.MessageName.RequestEvents,
                     'from_rank': self.rank,
                     'to_rank': 0,
                     'content': {'num_event_ranges': self.default_event_ranges},
                     'wait_for_response': True}
        req = MPIMessage(req_attrs)
        self.mpiservice.put(req)
        events = self.mpiservice.get_response(req)
        for event in events:
            self.events.append(event)

    def get_event_ranges(self, num_ranges=1):
        if len(self.events) < num_ranges:
            self.request_event_ranges()
        events = []
        for i in range(num_ranges):
            if len(self.events):
                events.append(self.events.pop(0))
        return events

    def handle_out_message(self, message):
        """
        Handle ES output or error messages.

        :param message: a dict of parsed message.
                        For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                        Fro 'failed' event ranges, it's {'id': <id>, 'status': 'finished', 'message': <full message>}.
        """
        self.output_messages.append(message)

    def report_out_message(self, force=False):
        if self.output_messages and force:
            output_message = self.output_messages
            self.output_messages = []
            req_attrs = {'message_type': MPIMessage.MessageType.Request,
                         'message_name': MPIMessage.MessageName.OutputFile,
                         'from_rank': self.rank,
                         'to_rank': 0,
                         'content': output_message,
                         'wait_for_response': True}
            req = MPIMessage(req_attrs)
            self.mpiservice.put(req)
            return self.mpiservice.get_response(req)

    def run_one_job(self):

        logging.debug('gettting payload')
        payload = self.get_payload()
        logging.debug('got payload: %s' % payload)

        logging.info('init ESProcess')
        process = ESProcess(payload)
        process.set_get_event_ranges_hook(self.get_event_ranges)
        process.set_handle_out_message_hook(self.handle_out_message)

        logging.info('ESProcess starts to run')
        process.start()
        while not self.is_stop():
            process.join(1)
            self.report_out_message()
        if self.is_stop():
            logging.info("Stop ESProcess.")
            process.stop()
            process.join()
        logging.info('ESProcess finishes')
        self.report_out_message(force=True)

    def run(self):
        while not self.is_stop():
            self.run_one_job()


class MPIManager(threading.Thread):
    def __init__(self, work_dir, rank, num_events_per_request=1):
        super(MPIManager, self).__init__()
        self.work_dir = work_dir
        self.rank = rank
        self.num_events_per_request = num_events_per_request  # should be mpisize * coresPerNode

        self.mpiservice = MPIService()

        self.init_processor()
        self.current_job = None
        self.rank_jobs = {}
        self.events = {}
        self.output_messages = []
        self.is_requesting_events = False

        self.__stop = threading.Event()

    def __del__(self):
        self.stop()
        if self.communication_manager:
            self.communication_manager.stop()

    def init_processor(self):
        self.processor = {MPIMessage.MessageName.RequestJob: self.get_jobs,
                          MPIMessage.MessageName.RequestEvents: self.get_events,
                          MPIMessage.MessageName.OutputFile: self.handle_output_message}

    def start(self):
        super(MPIManager, self).start()
        self.communication_manager = CommunicationManager()
        self.communication_manager.start()

    def stop(self):
        if not self.is_stop():
            self.__stop.set()

    def is_stop(self):
        return self.__stop.isSet()

    def stop_communicator(self):
        logging.info("Stopping communication manager")
        if self.communication_manager:
            while self.communication_manager.is_alive():
                if not self.communication_manager.is_stop():
                    self.communication_manager.stop()
        logging.info("Communication manager stopped")

    def cache_events_hook(self, events):
        for event in events:
            self.events.append(event)
        self.is_requesting_events = False

    def cache_events(self):
        if not self.current_job:
            return
        if len(self.events) < self.num_events_per_request:
            self.communication_manager.get_events(self.num_events_per_request, self.current_job, self.cache_events_hook)
            self.is_requesting_events = True

    def get_jobs(self, req):
        logging.info("Getting jobs for req: %s" % req)
        if not self.current_job:
            jobs = self.communication_manager.get_jobs(njobs=1, args=self.args)
            logging.info("Received jobs: %s" % jobs)
            if jobs:
                self.current_job = jobs[0]
        if self.rank not in self.rank_jobs.keys():
            self.rank_jobs[self.rank] = []

        resp_attr = req.generate_response()
        if self.current_job['pandaid'] not in self.rank_jobs[self.rank]:
            resp_attr['content'] = self.current_job
        else:
            resp_attr['content'] = None
        resp = MPIMessage(resp_attr)
        return resp

    def get_events(self, req):
        if len(self.events) < req.num_events:
            while self.is_requesting_events:
                time.sleep(0.1)

        events = []
        for i in range(req.num_events):
            if len(self.events):
                events.append(self.events.pop(0))
        resp_attr = req.generate_response()
        resp_attr['content'] = events
        return MPIMessage(resp_attr)

    def handle_output_message(self, req):
        for msg in req.content:
            self.output_messages.append(msg)

    def report_output_message(self):
        if self.output_messages and True:  # a time period to report it.
            output_messages = self.output_messages
            self.output_messages = []
            self.communication_manager.update_events(output_messages)

    def process_messages(self, req):
        logging.info("Processing request message: %s" % req)
        resp = self.processor[req.message_name](req)
        logging.info("Finished processing request message, response is: %s" % resp)
        return resp

    def run(self):
        while not self.is_stop():
            self.cache_events()
            req = self.mpiservice.get()
            resp = self.process_message(req)
            self.mpiservice.put(resp)
            self.report_output_message()
        self.report_output_message()


class MPIExecutor(threading.Thread):
    def __init__(self, **kwargs):
        super(MPIExecutor, self).__init__()
        self.exit_code = None

        if not hasattr(self, 'work_dir'):
            self.work_dir = None
        if not self.work_dir:
            self.work_dir = os.getcwd()

        if not hasattr(self, 'cores_per_node') or not self.cores_per_node:
            self.cores_per_node = 1

        self.run_instance = None

    def get_work_dir(self):
        return self.work_dir

    def set_retrieve_payload(self):
        self.__is_retrieve_payload = True

    def is_payload_started(self):
        return True

    def start(self):
        super(MPIExecutor, self).start()

    def stop(self):
        if self.run_instance:
            self.run_instance.stop()

    def get_pid(self):
        return os.getpid()

    def run_mpi_manager(self, work_dir, rank, num_events_per_request=1):
        log_file = os.path.join(work_dir, 'mpi_manager.log')
        logging.info("Redirect MPI manager logs to %s" % log_file)
        logging.basicConfig(filename=log_file, level=logging.INFO,
                            format='%(asctime)s | %(levelname)-8s | %(message)s')


        mpi_manager = MPIManager(work_dir, rank, num_events_per_request)
        mpi_manager.start()
        return mpi_manager

    def run_mpi_worker(self, work_dir, rank, default_event_ranges=1):
        log_file = os.path.join(work_dir, 'mpi_worker.log')
        logging.info("Redirect MPI manager logs to %s" % log_file)
        logging.basicConfig(filename=log_file, level=logging.INFO,
                            format='%(asctime)s | %(levelname)-8s | %(message)s')

        mpi_worker = MPIWorker(work_dir, rank, default_event_ranges)
        mpi_worker.start()
        return mpi_worker

    def run(self):
        """
        Initialize and run MPI manager and workers
        """
        try:
            comm = MPI.COMM_WORLD
            mpirank = comm.Get_rank()
            mpisize = comm.Get_size()
            logging.info("MPI rank: %s, MPI size: %s" % (mpirank, mpisize))

            work_dir = self.get_work_dir()
            work_dir = os.path.join(work_dir, 'rank_%s' % mpirank)
            if not os.path.exists(work_dir):
                os.makedirs(work_dir)
            os.chdir(work_dir)
            logging.info("Workdir: %s" % work_dir)

            if mpirank == 0:
                self.run_instance = self.run_mpi_manager(work_dir=work_dir, rank=mpirank, num_events_per_request=self.cores_per_node * (mpisize - 1))
            else:
                self.run_instance = self.run_mpi_worker(work_dir=work_dir, rank=mpirank, default_event_ranges=self.cores_per_node)

            while self.run_instance.is_alive():
                time.sleep(1)
        except Exception as e:
            mpirank = MPI.COMM_WORLD.Get_rank()
            logging.info("Rank %s: MPIExecutor caught an excpetion: %s, %s" % (mpirank, e, traceback.format_exc()))
            MPI.COMM_WORLD.Abort()
            self.exit_code = -1
            sys.exit(-1)

        self.exit_code = 0

    def get_exit_code(self):
        return self.exit_code
