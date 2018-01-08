#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

"""
Exceptions in pilot
"""

import traceback
from errorcodes import ErrorCodes
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
        self._message = "An unknown pilot exception occurred."
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
        return self._error_string + "\nStacktrace: %s" % self._stack_trace

    def get_error_code(self):
        return self._errorCode


class NotImplemented(PilotException):
    """
    NotImplemented
    """
    def __init__(self, *args, **kwargs):
        super(NotImplemented, self).__init__(args, kwargs)
        self._errorCode = errors.NOTIMPLEMENTED
        self._message = "The class or function is not implemented."


class UnknownException(PilotException):
    """
    Unknown exception.
    """
    def __init__(self, *args, **kwargs):
        super(UnknownException, self).__init__(args, kwargs)
        self._errorCode = errors.UNKNOWNEXCEPTION
        self._message = "An unknown pilot exception has occurred."


class NoLocalSpace(PilotException):
    """
    Not enough local space.
    """
    def __init__(self, *args, **kwargs):
        super(NoLocalSpace, self).__init__(args, kwargs)
        self._errorCode = 1098
        self._message = "Not enough local space."


class StageInFailure(PilotException):
    """
    Failed to stage-in file.
    """
    def __init__(self, *args, **kwargs):
        super(StageInFailure, self).__init__(args, kwargs)
        self._errorCode = 1099
        self._message = "Failed to stage-in file."


class StageOutFailure(PilotException):
    """
    Failed to stage-out file.
    """
    def __init__(self, *args, **kwargs):
        super(StageOutFailure, self).__init__(args, kwargs)
        self._errorCode = 1137
        self._message = "Failed to stage-out file."


class SetupFailure(PilotException):
    """
    Failed to setup environment.
    """
    def __init__(self, *args, **kwargs):
        super(SetupFailure, self).__init__(args, kwargs)
        self._errorCode = 1110
        self._message = "Failed to setup environment."


class RunPayloadFailure(PilotException):
    """
    Failed to execute payload.
    """
    def __init__(self, *args, **kwargs):
        super(RunPayloadFailure, self).__init__(args, kwargs)
        self._errorCode = 1111
        self._message = "Failed to execute payload."


class MessageFailure(PilotException):
    """
    Failed to handle messages.
    """
    def __init__(self, *args, **kwargs):
        super(MessageFailure, self).__init__(args, kwargs)
        self._errorCode = 1112
        self._message = "Failed to handle message from payload."


class FileHandlingFailure(PilotException):
    """
    Failed during file handling.
    """
    def __init__(self, *args, **kwargs):
        super(FileHandlingFailure, self).__init__(args, kwargs)
        self._errorCode = 1999
        self._message = "Failed during file handling."


class ConversionFailure(PilotException):
    """
    Failed to convert object data.
    """
    def __init__(self, *args, **kwargs):
        super(ConversionFailure, self).__init__(args, kwargs)
        self._errorCode = 1998
        self._message = "Failed to convert object data."


class MKDirFailure(PilotException):
    """
    Failed to create local directory.
    """
    def __init__(self, *args, **kwargs):
        super(MKDirFailure, self).__init__(args, kwargs)
        self._errorCode = 1997
        self._message = "Failed to create local directory."
