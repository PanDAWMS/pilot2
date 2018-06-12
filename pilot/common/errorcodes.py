#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

import re


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
    REPLICANOTFOUND = 1100
    NOSUCHFILE = 1103
    USERDIRTOOLARGE = 1104
    STDOUTTOOBIG = 1106
    SETUPFAILURE = 1110
    MKDIR = 1134
    STAGEOUTFAILED = 1137
    PUTMD5MISMATCH = 1141
    GETMD5MISMATCH = 1145
    TRFDOWNLOADFAILURE = 1149
    LOOPINGJOB = 1150
    STAGEINTIMEOUT = 1151  # called GETTIMEOUT in Pilot 1
    STAGEOUTTIMEOUT = 1152  # called PUTTIMEOUT in Pilot 1
    NOPROXY = 1163
    GETADMISMATCH = 1171
    PUTADMISMATCH = 1172
    NOVOMSPROXY = 1177

    # Error code constants (new since Pilot 2)
    NOTIMPLEMENTED = 1300
    UNKNOWNEXCEPTION = 1301
    CONVERSIONFAILURE = 1302
    FILEHANDLINGFAILURE = 1303
    MESSAGEHANDLINGFAILURE = 1304
    PAYLOADEXECUTIONFAILURE = 1305
    SINGULARITYGENERALFAILURE = 1306
    SINGULARITYNOLOOPDEVICES = 1307
    SINGULARITYBINDPOINTFAILURE = 1308
    SINGULARITYIMAGEMOUNTFAILURE = 1309
    PAYLOADEXECUTIONEXCEPTION = 1310

    _error_messages = {
        GENERALERROR: "General pilot error, consult batch log",
        NOLOCALSPACE: "Not enough local space",
        STAGEINFAILED: "Failed to stage-in file",
        REPLICANOTFOUND: "Replica not found",
        NOSUCHFILE: "No such file or directory",
        USERDIRTOOLARGE: "User work directory too large",
        STDOUTTOOBIG: "Payload log or stdout file too big",
        SETUPFAILURE: "Failed during payload setup",
        MKDIR: "Failed to create local directory",
        STAGEOUTFAILED: "Failed to stage-out file",
        PUTMD5MISMATCH: "md5sum mismatch on output file",
        GETMD5MISMATCH: "md5sum mismatch on input file",
        TRFDOWNLOADFAILURE: "Transform could not be downloaded",
        LOOPINGJOB: "Looping job killed by pilot",
        STAGEINTIMEOUT: "File transfer timed out during stage-in",
        STAGEOUTTIMEOUT: "File transfer timed out during stage-out",
        NOPROXY: "Grid proxy not valid",
        GETADMISMATCH: "adler32 mismatch on input file",
        PUTADMISMATCH: "adler32 mismatch on output file",
        NOVOMSPROXY: "Voms proxy not valid",
        NOTIMPLEMENTED: "The class or function is not implemented",
        UNKNOWNEXCEPTION: "An unknown pilot exception has occurred",
        CONVERSIONFAILURE: "Failed to convert object data",
        FILEHANDLINGFAILURE: "Failed during file handling",
        MESSAGEHANDLINGFAILURE: "Failed to handle message from payload",
        PAYLOADEXECUTIONFAILURE: "Failed to execute payload",
        SINGULARITYGENERALFAILURE: "Singularity: general failure",
        SINGULARITYNOLOOPDEVICES: "Singularity: No more available loop devices",
        SINGULARITYBINDPOINTFAILURE: "Singularity: Not mounting requested bind point",
        SINGULARITYIMAGEMOUNTFAILURE: "Singularity: Failed to mount image",
        PAYLOADEXECUTIONEXCEPTION: "Exception caught during payload execution"
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

    def add_error_code(self, errorcode, pilot_error_codes=[], pilot_error_diags=[]):
        """
        Add pilot error code to list of error codes.
        This function adds the given error code to the list of all errors that have occurred. This is needed since
        several errors can happen; e.g. a stage-in error can be followed by a stage-out error during the log transfer.
        The full list of errors is dumped to the log, but only the first error is reported to the server.
        The function also sets the corresponding error message.

        :param errorcode: pilot error code (integer)
        :param pilot_error_codes: list of pilot error codes (list of integers)
        :param pilot_error_diags: list of pilot error diags (list of strings)
        :return: pilotErrorCodes, pilotErrorDiags
        """

        # do nothing if the error code has already been added
        if errorcode not in pilot_error_codes:
            pilot_error_codes.append(errorcode)
            pilot_error_diags.append(self.get_error_message(errorcode))

        return pilot_error_codes, pilot_error_diags

    def report_errors(self, pilot_error_codes, pilot_error_diags):
        """
        Report all errors that occurred during running.
        The function should be called towards the end of running a job.

        :param pilot_error_codes: list of pilot error codes (list of integers)
        :param pilot_error_diags: list of pilot error diags (list of strings)
        :return: error_report (string)
        """

        i = 0
        if pilot_error_codes == []:
            report = "no pilot errors were reported"
        else:
            report = "Nr.\tError code\tError diagnostics"
            for errorcode in pilot_error_codes:
                i += 1
                report += "\n%d.\t%d\t%s" % (i, errorcode, pilot_error_diags[i - 1])

        return report

    def resolve_transform_error(self, exit_code, stderr):
        """
        Assign a pilot error code to a specific transform error.
        :param exit_code: transform exit code.
        :param stderr: transform stderr
        :return: pilot error code (int)
        """

        if exit_code == 251 and "Not mounting requested bind point" in stderr:
            ec = self.SINGULARITYBINDPOINTFAILURE
        elif exit_code == 255 and "No more available loop devices" in stderr:
            ec = self.SINGULARITYNOLOOPDEVICES
        elif exit_code == 255 and "Failed to mount image" in stderr:
            ec = self.SINGULARITYIMAGEMOUNTFAILURE
        else:
            # do not assign a pilot error code for unidentified transform error, return 0
            ec = 0

        return ec

    def extract_stderr_msg(self, stderr):
        """
        Extract the ERROR or WARNING message from the singularity stderr.
        :param stderr: string.
        :return: string.
        """

        msg = ""
        pattern = r"ERROR +\: (.+)"
        found = re.findall(pattern, stderr)
        if len(found) > 0:
            msg = found[0]
        else:
            pattern = r"WARNING\: (.+)"
            found = re.findall(pattern, stderr)
            if len(found) > 0:
                msg = found[0]

        return msg
