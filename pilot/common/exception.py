#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019

"""
Exceptions in pilot
"""

import time
import threading
import traceback
from sys import exc_info, version_info

from .errorcodes import ErrorCodes
errors = ErrorCodes()


def is_python3():
    """
    Check if we are running on Python 3.

    :return: boolean.
    """

    return version_info >= (3, 0)


class PilotException(Exception):
    """
    The basic exception class.
    The pilot error code can be defined here, where the pilot error code will
    be propageted to job server.
    """

    def __init__(self, *args, **kwargs):
        super(PilotException, self).__init__(args, kwargs)
        self.args = args
        self.kwargs = kwargs
        code = self.kwargs.get('code', None)
        if code:
            self._errorCode = code
        else:
            self._errorCode = errors.UNKNOWNEXCEPTION
        self._message = errors.get_error_message(self._errorCode)
        self._error_string = None
        self._stack_trace = "%s" % traceback.format_exc()

    def __str__(self):
        try:
            self._error_string = "error code: %s, message: %s" % (self._errorCode, self._message % self.kwargs)
        except Exception:
            # at least get the core message out if something happened
            self._error_string = "error code: %s, message: %s" % (self._errorCode, self._message)

        if len(self.args) > 0:
            # If there is a non-kwarg parameter, assume it's the error
            # message or reason description and tack it on to the end
            # of the exception message
            # Convert all arguments into their string representations...
            try:
                args = ["%s" % arg for arg in self.args if arg]
            except Exception:
                args = ["{}".format(self.args)]
            self._error_string = (self._error_string + "\ndetails: %s" % '\n'.join(args))
        return self._error_string.strip()

    def get_detail(self):
        try:
            self._error_string = "error code: %s, message: %s" % (self._errorCode, self._message % self.kwargs)
        except Exception:
            # at least get the core message out if something happened
            self._error_string = "error code: %s, message: %s" % (self._errorCode, self._message)

        return self._error_string + "\nstacktrace: %s" % self._stack_trace

    def get_error_code(self):
        return self._errorCode

    def get_last_error(self):
        if self.args:
            return self.args[-1]
        return self._message


class NotImplemented(PilotException):
    """
    Not implemented exception.
    """
    def __init__(self, *args, **kwargs):
        super(NotImplemented, self).__init__(args, kwargs)
        self._errorCode = errors.NOTIMPLEMENTED
        self._message = errors.get_error_message(self._errorCode)


class UnknownException(PilotException):
    """
    Unknown exception.
    """
    def __init__(self, *args, **kwargs):
        super(UnknownException, self).__init__(args, kwargs)
        self._errorCode = errors.UNKNOWNEXCEPTION
        self._message = errors.get_error_message(self._errorCode)


class NoLocalSpace(PilotException):
    """
    Not enough local space.
    """
    def __init__(self, *args, **kwargs):
        super(NoLocalSpace, self).__init__(args, kwargs)
        self._errorCode = errors.NOLOCALSPACE
        self._message = errors.get_error_message(self._errorCode)


class SizeTooLarge(PilotException):
    """
    Too large input files.
    """
    def __init__(self, *args, **kwargs):
        super(SizeTooLarge, self).__init__(args, kwargs)
        self._errorCode = errors.SIZETOOLARGE
        self._message = errors.get_error_message(self._errorCode)


class StageInFailure(PilotException):
    """
    Failed to stage-in file.
    """
    def __init__(self, *args, **kwargs):
        super(StageInFailure, self).__init__(args, kwargs)
        self._errorCode = errors.STAGEINFAILED
        self._message = errors.get_error_message(self._errorCode)


class StageOutFailure(PilotException):
    """
    Failed to stage-out file.
    """
    def __init__(self, *args, **kwargs):
        super(StageOutFailure, self).__init__(args, kwargs)
        self._errorCode = errors.STAGEOUTFAILED
        self._message = errors.get_error_message(self._errorCode)


class SetupFailure(PilotException):
    """
    Failed to setup environment.
    """
    def __init__(self, *args, **kwargs):
        super(SetupFailure, self).__init__(args, kwargs)
        self._errorCode = errors.SETUPFAILURE
        self._message = errors.get_error_message(self._errorCode)


class RunPayloadFailure(PilotException):
    """
    Failed to execute payload.
    """
    def __init__(self, *args, **kwargs):
        super(RunPayloadFailure, self).__init__(args, kwargs)
        self._errorCode = errors.PAYLOADEXECUTIONFAILURE
        self._message = errors.get_error_message(self._errorCode)


class MessageFailure(PilotException):
    """
    Failed to handle messages.
    """
    def __init__(self, *args, **kwargs):
        super(MessageFailure, self).__init__(args, kwargs)
        self._errorCode = errors.MESSAGEHANDLINGFAILURE
        self._message = errors.get_error_message(self._errorCode)


class CommunicationFailure(PilotException):
    """
    Failed to communicate with servers such as Panda, Harvester, ACT and so on.
    """
    def __init__(self, *args, **kwargs):
        super(CommunicationFailure, self).__init__(args, kwargs)
        self._errorCode = errors.COMMUNICATIONFAILURE
        self._message = errors.get_error_message(self._errorCode)


class FileHandlingFailure(PilotException):
    """
    Failed during file handling.
    """
    def __init__(self, *args, **kwargs):
        super(FileHandlingFailure, self).__init__(args, kwargs)
        self._errorCode = errors.FILEHANDLINGFAILURE
        self._message = errors.get_error_message(self._errorCode)


class NoSuchFile(PilotException):
    """
    No such file or directory.
    """
    def __init__(self, *args, **kwargs):
        super(NoSuchFile, self).__init__(args, kwargs)
        self._errorCode = errors.NOSUCHFILE
        self._message = errors.get_error_message(self._errorCode)


class ConversionFailure(PilotException):
    """
    Failed to convert object data.
    """
    def __init__(self, *args, **kwargs):
        super(ConversionFailure, self).__init__(args, kwargs)
        self._errorCode = errors.CONVERSIONFAILURE
        self._message = errors.get_error_message(self._errorCode)


class MKDirFailure(PilotException):
    """
    Failed to create local directory.
    """
    def __init__(self, *args, **kwargs):
        super(MKDirFailure, self).__init__(args, kwargs)
        self._errorCode = errors.MKDIR
        self._message = errors.get_error_message(self._errorCode)


class NoGridProxy(PilotException):
    """
    Grid proxy not valid.
    """
    def __init__(self, *args, **kwargs):
        super(NoGridProxy, self).__init__(args, kwargs)
        self._errorCode = errors.NOPROXY
        self._message = errors.get_error_message(self._errorCode)


class NoVomsProxy(PilotException):
    """
    Voms proxy not valid.
    """
    def __init__(self, *args, **kwargs):
        super(NoVomsProxy, self).__init__(args, kwargs)
        self._errorCode = errors.NOVOMSPROXY
        self._message = errors.get_error_message(self._errorCode)


class TrfDownloadFailure(PilotException):
    """
    Transform could not be downloaded.
    """
    def __init__(self, *args, **kwargs):
        super(TrfDownloadFailure, self).__init__(args, kwargs)
        self._errorCode = errors.TRFDOWNLOADFAILURE
        self._message = errors.get_error_message(self._errorCode)


class NotDefined(PilotException):
    """
    Not defined exception.
    """
    def __init__(self, *args, **kwargs):
        super(NotDefined, self).__init__(args, kwargs)
        self._errorCode = errors.NOTDEFINED
        self._message = errors.get_error_message(self._errorCode)


class NotSameLength(PilotException):
    """
    Not same length exception.
    """
    def __init__(self, *args, **kwargs):
        super(NotSameLength, self).__init__(args, kwargs)
        self._errorCode = errors.NOTSAMELENGTH
        self._message = errors.get_error_message(self._errorCode)


class ESRecoverable(PilotException):
    """
    Eventservice recoverable exception.
    """
    def __init__(self, *args, **kwargs):
        super(ESRecoverable, self).__init__(args, kwargs)
        self._errorCode = errors.ESRECOVERABLE
        self._message = errors.get_error_message(self._errorCode)


class ESFatal(PilotException):
    """
    Eventservice fatal exception.
    """
    def __init__(self, *args, **kwargs):
        super(ESFatal, self).__init__(args, kwargs)
        self._errorCode = errors.ESFATAL
        self._message = errors.get_error_message(self._errorCode)


class ExecutedCloneJob(PilotException):
    """
    Clone job executed exception.
    """
    def __init__(self, *args, **kwargs):
        super(ExecutedCloneJob, self).__init__(args, kwargs)
        self._errorCode = errors.EXECUTEDCLONEJOB
        self._message = errors.get_error_message(self._errorCode)


class ESNoEvents(PilotException):
    """
    Eventservice no events exception.
    """
    def __init__(self, *args, **kwargs):
        super(ESNoEvents, self).__init__(args, kwargs)
        self._errorCode = errors.ESNOEVENTS
        self._message = errors.get_error_message(self._errorCode)


class ExceededMaxWaitTime(PilotException):
    """
    Exceeded maximum waiting time (after abort_job has been set).
    """
    def __init__(self, *args, **kwargs):
        super(ExceededMaxWaitTime, self).__init__(args, kwargs)
        self._errorCode = errors.EXCEEDEDMAXWAITTIME
        self._message = errors.get_error_message(self._errorCode)


class BadXML(PilotException):
    """
    Badly formed XML.
    """
    def __init__(self, *args, **kwargs):
        super(BadXML, self).__init__(args, kwargs)
        self._errorCode = errors.BADXML
        self._message = errors.get_error_message(self._errorCode)


class NoSoftwareDir(PilotException):
    """
    Software applications directory does not exist.
    """
    def __init__(self, *args, **kwargs):
        super(NoSoftwareDir, self).__init__(args, kwargs)
        self._errorCode = errors.NOSOFTWAREDIR
        self._message = errors.get_error_message(self._errorCode)


class LogFileCreationFailure(PilotException):
    """
    Log file could not be created.
    """
    def __init__(self, *args, **kwargs):
        super(LogFileCreationFailure, self).__init__(args, kwargs)
        self._errorCode = errors.LOGFILECREATIONFAILURE
        self._message = errors.get_error_message(self._errorCode)


class QueuedataFailure(PilotException):
    """
    Failed to download queuedata.
    """
    def __init__(self, *args, **kwargs):
        super(QueuedataFailure, self).__init__(args, kwargs)
        self._errorCode = errors.QUEUEDATA
        self._message = errors.get_error_message(self._errorCode)


class QueuedataNotOK(PilotException):
    """
    Corrupt queuedata.
    """
    def __init__(self, *args, **kwargs):
        super(QueuedataNotOK, self).__init__(args, kwargs)
        self._errorCode = errors.QUEUEDATANOTOK
        self._message = errors.get_error_message(self._errorCode)


class ReplicasNotFound(PilotException):
    """
    No matching replicas were found in list_replicas() output.
    """
    def __init__(self, *args, **kwargs):
        super(ReplicasNotFound, self).__init__(args, kwargs)
        self._errorCode = errors.NOREPLICAS
        self._message = errors.get_error_message(self._errorCode)


class JobAlreadyRunning(PilotException):
    """
    Job is already running elsewhere.
    """
    def __init__(self, *args, **kwargs):
        super(JobAlreadyRunning, self).__init__(args, kwargs)
        self._errorCode = errors.JOBALREADYRUNNING
        self._message = errors.get_error_message(self._errorCode)


class ExcThread(threading.Thread):
    """
    Support class that allows for catching exceptions in threads.
    """

    def __init__(self, bucket, target, kwargs, name):
        """
        Init function with a bucket that can be used to communicate exceptions to the caller.
        The bucket is a Queue.queue() or queue.Queue() object that can hold an exception thrown by a thread.

        :param bucket: queue based bucket.
        :param target: target function to execute.
        :param kwargs: target function options.
        """
        threading.Thread.__init__(self, target=target, kwargs=kwargs, name=name)
        self.name = name
        self.bucket = bucket

    def run(self):
        """
        Thread run function.
        Any exceptions in the threads are caught in this function and placed in the bucket of the current thread.
        The bucket will be emptied by the control module that launched the thread. E.g. an exception is thrown in
        the retrieve thread (in function retrieve()) that is created by the job.control thread. The exception is caught
        by the run() function and placed in the bucket belonging to the retrieve thread. The bucket is emptied in
        job.control().

        :return:
        """
        try:
            if is_python3():
                self._target(**self._kwargs)
            else:
                self._Thread__target(**self._Thread__kwargs)
        except Exception:
            # logger object can't be used here for some reason:
            # IOError: [Errno 2] No such file or directory: '/state/partition1/scratch/PanDA_Pilot2_*/pilotlog.txt'
            print('exception caught by thread run() function: %s' % str(exc_info()))
            print(traceback.format_exc())
            print(traceback.print_tb(exc_info()[2]))
            self.bucket.put(exc_info())
            print("exception has been put in bucket queue belonging to thread \'%s\'" % self.name)
            if is_python3():
                args = self._kwargs.get('args', None)
            else:
                args = self._Thread__kwargs.get('args', None)
            if args:
                # the sleep is needed to allow the threads to catch up
                print('setting graceful stop in 10 s since there is no point in continuing')
                time.sleep(10)
                args.graceful_stop.set()

    def get_bucket(self):
        """
        Return the bucket object that holds any information about thrown exceptions.

        :return: bucket (Queue object)
        """
        return self.bucket
