#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

class ErrorCodes:
    """
    Pilot error codes.

    Note: Error code numbering is the same as in Pilot 1 since that is expected by the PanDA server and monitor.
    Note 2: Add error codes as they are needed in other modules. Do not import the full Pilot 1 list at once as there
    might very well be codes that can be reassigned/removed.
    """

    # Error code constants (from Pilot 1)
    GENERALERROR = 1008
    NOLOCALSPACE = 1098
    STAGEINFAILED = 1099
    SETUPFAILURE = 1110
    MKDIR = 1134
    STAGEOUTFAILED = 1137

    # Error code constants (new since Pilot 2)
    NOTIMPLEMENTED = 1300
    UNKNOWNEXCEPTION = 1301
    CONVERSIONFAILURE = 1302
    FILEHANDLINGFAILURE = 1303
    MESSAGEHANDLINGFAILURE = 1304
    PAYLOADEXECUTIONFAILURE = 1305

    _error_messages = {
        GENERALERROR: "General pilot error, consult batch log",
        NOTIMPLEMENTED: "The class or function is not implemented",
        UNKNOWNEXCEPTION: "An unknown pilot exception has occurred",
        NOLOCALSPACE: "Not enough local space",
        STAGEINFAILED: "Failed to stage-in file",
        SETUPFAILURE: "Failed during payload setup",
        MKDIR: "Failed to create local directory",
        STAGEOUTFAILED: "Failed to stage-out file",
        CONVERSIONFAILURE: "Failed to convert object data",
        FILEHANDLINGFAILURE: "Failed during file handling",
        MESSAGEHANDLINGFAILURE: "Failed to handle message from payload",
        PAYLOADEXECUTIONFAILURE: "Failed to execute payload",
    }


    def get_error_message(self, errorcode):
        """
        Return the error message corresponding to the given error code.

        :param errorcode:
        :return: errormessage (string)
        """

        if errorcode in self._error_messages:
            return self._error_messages[errorcode]
        else:
            return "Unknown error code: %d" % errorcode
