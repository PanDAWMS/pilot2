#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017-2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

"""
Exceptions in pilot
"""

import time
import threading
import traceback
from sys import exc_info

from .errorcodes import ErrorCodes
errors = ErrorCodes()


class PilotException(Exception):
    """
    The basic exception class.
    The pilot error code can be defined here, where the pilot error code will
    be propageted to job server.
    """

    def __init__(self, *args, **kwargs):
        super(PilotException, self).__init__(args, kwargs)
        self._errorCode = errors.UNKNOWNEXCEPTION
        self._message = errors.get_error_message(self._errorCode)
        self.args = args
        self.kwargs = kwargs
        self._error_string = None
        self._stack_trace = "%s" % traceback.format_exc()

    def __str__(self):
        try:
            self._error_string = "Error code: %s, message: %s" % (self._errorCode, self._message % self.kwargs)
        except Exception:
            # at least get the core message out if something happened
            self._error_string = "Error code: %s, message: %s" % (self._errorCode, self._message)

        if len(self.args) > 0:
            # If there is a non-kwarg parameter, assume it's the error
            # message or reason description and tack it on to the end
            # of the exception message
            # Convert all arguments into their string representations...
            args = ["%s" % arg for arg in self.args if arg]
            self._error_string = (self._error_string + "\nDetails: %s" % '\n'.join(args))
        return self._error_string.strip()

    def get_detail(self):
        try:
            self._error_string = "Error code: %s, message: %s" % (self._errorCode, self._message % self.kwargs)
        except Exception:
            # at least get the core message out if something happened
            self._error_string = "Error code: %s, message: %s" % (self._errorCode, self._message)

        return self._error_string + "\nStacktrace: %s" % self._stack_trace

    def get_error_code(self):
        return self._errorCode


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
            self._Thread__target(**self._Thread__kwargs)
        except Exception:
            # logger object can't be used here for some reason:
            # IOError: [Errno 2] No such file or directory: '/state/partition1/scratch/PanDA_Pilot2_*/pilotlog.txt'
            print('exception caught by thread run() function: %s' % str(exc_info()))
            print(traceback.format_exc())
            print(traceback.print_tb(exc_info()[2]))
            self.bucket.put(exc_info())
            print("exception has been put in bucket queue belonging to thread \'%s\'" % self.name)
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
