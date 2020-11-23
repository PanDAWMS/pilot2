#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2019
# - Wen Guan, wen.guan@cern.ch, 2018

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
    NFSSQLITE = 1115
    QUEUEDATA = 1116
    QUEUEDATANOTOK = 1117
    OUTPUTFILETOOLARGE = 1124
    NOSTORAGE = 1133
    STAGEOUTFAILED = 1137
    PUTMD5MISMATCH = 1141
    CHMODTRF = 1143
    PANDAKILL = 1144
    GETMD5MISMATCH = 1145
    TRFDOWNLOADFAILURE = 1149
    LOOPINGJOB = 1150
    STAGEINTIMEOUT = 1151  # called GETTIMEOUT in Pilot 1
    STAGEOUTTIMEOUT = 1152  # called PUTTIMEOUT in Pilot 1
    NOPROXY = 1163
    MISSINGOUTPUTFILE = 1165
    SIZETOOLARGE = 1168
    GETADMISMATCH = 1171
    PUTADMISMATCH = 1172
    NOVOMSPROXY = 1177
    GETGLOBUSSYSERR = 1180
    PUTGLOBUSSYSERR = 1181
    NOSOFTWAREDIR = 1186
    NOPAYLOADMETADATA = 1187
    LFNTOOLONG = 1190
    ZEROFILESIZE = 1191
    MKDIR = 1199
    KILLSIGNAL = 1200
    SIGTERM = 1201
    SIGQUIT = 1202
    SIGSEGV = 1203
    SIGXCPU = 1204
    USERKILL = 1205  # reserved error code, currently not used by pilot
    SIGBUS = 1206
    SIGUSR1 = 1207
    MISSINGINSTALLATION = 1211
    PAYLOADOUTOFMEMORY = 1212
    REACHEDMAXTIME = 1213
    UNKNOWNPAYLOADFAILURE = 1220
    FILEEXISTS = 1221
    BADALLOC = 1223
    ESRECOVERABLE = 1224
    ESFATAL = 1228
    EXECUTEDCLONEJOB = 1234
    PAYLOADEXCEEDMAXMEM = 1235
    FAILEDBYSERVER = 1236
    ESNOEVENTS = 1238
    MESSAGEHANDLINGFAILURE = 1240
    CHKSUMNOTSUP = 1242
    NORELEASEFOUND = 1244
    NOUSERTARBALL = 1246
    BADXML = 1247

    # Error code constants (new since Pilot 2)
    NOTIMPLEMENTED = 1300
    UNKNOWNEXCEPTION = 1301
    CONVERSIONFAILURE = 1302
    FILEHANDLINGFAILURE = 1303
    PAYLOADEXECUTIONFAILURE = 1305
    SINGULARITYGENERALFAILURE = 1306
    SINGULARITYNOLOOPDEVICES = 1307
    SINGULARITYBINDPOINTFAILURE = 1308
    SINGULARITYIMAGEMOUNTFAILURE = 1309
    PAYLOADEXECUTIONEXCEPTION = 1310
    NOTDEFINED = 1311
    NOTSAMELENGTH = 1312
    NOSTORAGEPROTOCOL = 1313
    UNKNOWNCHECKSUMTYPE = 1314
    UNKNOWNTRFFAILURE = 1315
    RUCIOSERVICEUNAVAILABLE = 1316
    EXCEEDEDMAXWAITTIME = 1317
    COMMUNICATIONFAILURE = 1318
    INTERNALPILOTPROBLEM = 1319
    LOGFILECREATIONFAILURE = 1320
    RUCIOLOCATIONFAILED = 1321
    RUCIOLISTREPLICASFAILED = 1322
    UNKNOWNCOPYTOOL = 1323
    SERVICENOTAVAILABLE = 1324
    SINGULARITYNOTINSTALLED = 1325
    NOREPLICAS = 1326
    UNREACHABLENETWORK = 1327
    PAYLOADSIGSEGV = 1328
    NONDETERMINISTICDDM = 1329
    JSONRETRIEVALTIMEOUT = 1330
    MISSINGINPUTFILE = 1331
    BLACKHOLE = 1332
    NOREMOTESPACE = 1333
    ATLASSETUPFATAL = 1334
    MISSINGUSERCODE = 1335
    JOBALREADYRUNNING = 1336
    BADMEMORYMONITORJSON = 1337
    STAGEINAUTHENTICATIONFAILURE = 1338
    DBRELEASEFAILURE = 1339
    SINGULARITYNEWUSERNAMESPACE = 1340
    BADQUEUECONFIGURATION = 1341
    MIDDLEWAREIMPORTFAILURE = 1342
    NOOUTPUTINJOBREPORT = 1343
    RESOURCEUNAVAILABLE = 1344
    SINGULARITYFAILEDUSERNAMESPACE = 1345
    TRANSFORMNOTFOUND = 1346
    UNSUPPORTEDSL5OS = 1347
    SINGULARITYRESOURCEUNAVAILABLE = 1348
    UNRECOGNIZEDTRFARGUMENTS = 1349
    EMPTYOUTPUTFILE = 1350
    UNRECOGNIZEDTRFSTDERR = 1351
    STATFILEPROBLEM = 1352
    NOSUCHPROCESS = 1353
    GENERALCPUCALCPROBLEM = 1354
    COREDUMP = 1355
    PREPROCESSFAILURE = 1356
    POSTPROCESSFAILURE = 1357
    MISSINGRELEASEUNPACKED = 1358
    PANDAQUEUENOTACTIVE = 1359
    IMAGENOTFOUND = 1360
    REMOTEFILECOULDNOTBEOPENED = 1361
    XRDCPERROR = 1362

    _error_messages = {
        GENERALERROR: "General pilot error, consult batch log",
        NOLOCALSPACE: "Not enough local space",
        STAGEINFAILED: "Failed to stage-in file",
        REPLICANOTFOUND: "Replica not found",
        NOSUCHFILE: "No such file or directory",
        USERDIRTOOLARGE: "User work directory too large",
        STDOUTTOOBIG: "Payload log or stdout file too big",
        SETUPFAILURE: "Failed during payload setup",
        NFSSQLITE: "NFS SQLite locking problems",
        QUEUEDATA: "Pilot could not download queuedata",
        QUEUEDATANOTOK: "Pilot found non-valid queuedata",
        OUTPUTFILETOOLARGE: "Output file too large",
        NOSTORAGE: "Fetching default storage failed: no activity related storage defined",
        STAGEOUTFAILED: "Failed to stage-out file",
        PUTMD5MISMATCH: "md5sum mismatch on output file",
        GETMD5MISMATCH: "md5sum mismatch on input file",
        CHMODTRF: "Failed to chmod transform",
        PANDAKILL: "This job was killed by panda server",
        MISSINGOUTPUTFILE: "Local output file is missing",
        SIZETOOLARGE: "Total file size too large",
        TRFDOWNLOADFAILURE: "Transform could not be downloaded",
        LOOPINGJOB: "Looping job killed by pilot",
        STAGEINTIMEOUT: "File transfer timed out during stage-in",
        STAGEOUTTIMEOUT: "File transfer timed out during stage-out",
        NOPROXY: "Grid proxy not valid",
        GETADMISMATCH: "adler32 mismatch on input file",
        PUTADMISMATCH: "adler32 mismatch on output file",
        NOVOMSPROXY: "Voms proxy not valid",
        GETGLOBUSSYSERR: "Globus system error during stage-in",
        PUTGLOBUSSYSERR: "Globus system error during stage-out",
        NOSOFTWAREDIR: "Software directory does not exist",
        NOPAYLOADMETADATA: "Payload metadata does not exist",
        LFNTOOLONG: "LFN too long (exceeding limit of 255 characters)",
        ZEROFILESIZE: "File size cannot be zero",
        MKDIR: "Failed to create local directory",
        KILLSIGNAL: "Job terminated by unknown kill signal",
        SIGTERM: "Job killed by signal: SIGTERM",
        SIGQUIT: "Job killed by signal: SIGQUIT",
        SIGSEGV: "Job killed by signal: SIGSEGV",
        SIGXCPU: "Job killed by signal: SIGXCPU",
        SIGUSR1: "Job killed by signal: SIGUSR1",
        SIGBUS: "Job killed by signal: SIGBUS",
        USERKILL: "Job killed by user",
        MISSINGINSTALLATION: "Missing installation",
        PAYLOADOUTOFMEMORY: "Payload ran out of memory",
        REACHEDMAXTIME: "Reached batch system time limit",
        UNKNOWNPAYLOADFAILURE: "Job failed due to unknown reason (consult log file)",
        FILEEXISTS: "File already exists",
        BADALLOC: "Transform failed due to bad_alloc",
        CHKSUMNOTSUP: "Query checksum is not supported",
        NORELEASEFOUND: "No release candidates found",
        NOUSERTARBALL: "User tarball could not be downloaded from PanDA server",
        BADXML: "Badly formed XML",
        ESRECOVERABLE: "Event service: recoverable error",
        ESFATAL: "Event service: fatal error",
        EXECUTEDCLONEJOB: "Clone job is already executed",
        PAYLOADEXCEEDMAXMEM: "Payload exceeded maximum allowed memory",
        FAILEDBYSERVER: "Failed by server",
        ESNOEVENTS: "Event service: no events",
        MESSAGEHANDLINGFAILURE: "Failed to handle message from payload",
        NOTIMPLEMENTED: "The class or function is not implemented",
        UNKNOWNEXCEPTION: "An unknown pilot exception has occurred",
        CONVERSIONFAILURE: "Failed to convert object data",
        FILEHANDLINGFAILURE: "Failed during file handling",
        PAYLOADEXECUTIONFAILURE: "Failed to execute payload",
        SINGULARITYGENERALFAILURE: "Singularity: general failure",
        SINGULARITYNOLOOPDEVICES: "Singularity: No more available loop devices",
        SINGULARITYBINDPOINTFAILURE: "Singularity: Not mounting requested bind point",
        SINGULARITYIMAGEMOUNTFAILURE: "Singularity: Failed to mount image",
        SINGULARITYNOTINSTALLED: "Singularity: not installed",
        PAYLOADEXECUTIONEXCEPTION: "Exception caught during payload execution",
        NOTDEFINED: "Not defined",
        NOTSAMELENGTH: "Not same length",
        NOSTORAGEPROTOCOL: "No protocol defined for storage endpoint",
        UNKNOWNCHECKSUMTYPE: "Unknown checksum type",
        UNKNOWNTRFFAILURE: "Unknown transform failure",
        RUCIOSERVICEUNAVAILABLE: "Rucio: Service unavailable",
        EXCEEDEDMAXWAITTIME: "Exceeded maximum waiting time",
        COMMUNICATIONFAILURE: "Failed to communicate with server",
        INTERNALPILOTPROBLEM: "An internal Pilot problem has occurred (consult Pilot log)",
        LOGFILECREATIONFAILURE: "Failed during creation of log file",
        RUCIOLOCATIONFAILED: "Failed to get client location for Rucio",
        RUCIOLISTREPLICASFAILED: "Failed to get replicas from Rucio",
        UNKNOWNCOPYTOOL: "Unknown copy tool",
        SERVICENOTAVAILABLE: "Service not available at the moment",
        NOREPLICAS: "No matching replicas were found in list_replicas() output",
        UNREACHABLENETWORK: "Unable to stage-in file since network is unreachable",
        PAYLOADSIGSEGV: "SIGSEGV: Invalid memory reference or a segmentation fault",
        NONDETERMINISTICDDM: "Failed to construct SURL for non-deterministic ddm (update AGIS)",
        JSONRETRIEVALTIMEOUT: "JSON retrieval timed out",
        MISSINGINPUTFILE: "Input file is missing in storage element",
        BLACKHOLE: "Black hole detected in file system (consult Pilot log)",
        NOREMOTESPACE: "No space left on device",
        ATLASSETUPFATAL: "AtlasSetup failed with a fatal exception (consult Payload log)",
        MISSINGUSERCODE: "User code not available on PanDA server (resubmit task with --useNewCode)",
        JOBALREADYRUNNING: "Job is already running elsewhere",
        BADMEMORYMONITORJSON: "Memory monitor produced bad output",
        STAGEINAUTHENTICATIONFAILURE: "Authentication failure during stage-in",
        DBRELEASEFAILURE: "Local DBRelease handling failed (consult Pilot log)",
        SINGULARITYNEWUSERNAMESPACE: "Singularity: Failed invoking the NEWUSER namespace runtime",
        BADQUEUECONFIGURATION: "Bad queue configuration detected",
        MIDDLEWAREIMPORTFAILURE: "Failed to import middleware (consult Pilot log)",
        NOOUTPUTINJOBREPORT: "Found no output in job report",
        RESOURCEUNAVAILABLE: "Resource temporarily unavailable",
        SINGULARITYFAILEDUSERNAMESPACE: "Singularity: Failed to create user namespace",
        TRANSFORMNOTFOUND: "Transform not found",
        UNSUPPORTEDSL5OS: "Unsupported SL5 OS",
        SINGULARITYRESOURCEUNAVAILABLE: "Singularity: Resource temporarily unavailable",  # not the same as RESOURCEUNAVAILABLE
        UNRECOGNIZEDTRFARGUMENTS: "Unrecognized transform arguments",
        EMPTYOUTPUTFILE: "Empty output file detected",
        UNRECOGNIZEDTRFSTDERR: "Unrecognized fatal error in transform stderr",
        STATFILEPROBLEM: "Failed to stat proc file for CPU consumption calculation",
        NOSUCHPROCESS: "CPU consumption calculation failed: No such process",
        GENERALCPUCALCPROBLEM: "General CPU consumption calculation problem (consult Pilot log)",
        COREDUMP: "Core dump detected",
        PREPROCESSFAILURE: "Pre-process command failed",
        POSTPROCESSFAILURE: "Post-process command failed",
        MISSINGRELEASEUNPACKED: "Missing release setup in unpacked container",
        PANDAQUEUENOTACTIVE: "PanDA queue is not active",
        IMAGENOTFOUND: "Image not found",
        REMOTEFILECOULDNOTBEOPENED: "Remote file could not be opened",
        XRDCPERROR: "Xrdcp was unable to open file"
    }

    put_error_codes = [1135, 1136, 1137, 1141, 1152, 1181]
    recoverable_error_codes = [0] + put_error_codes

    def get_kill_signal_error_code(self, signal):
        """
        Match a kill signal with a corresponding Pilot error code.

        :param signal: signal name (string).
        :return: Pilot error code (integer).
        """

        signals_dictionary = {'SIGTERM': self.SIGTERM,
                              'SIGQUIT': self.SIGQUIT,
                              'SIGSEGV': self.SIGSEGV,
                              'SIGXCPU': self.SIGXCPU,
                              'SIGUSR1': self.SIGUSR1,
                              'SIGBUS': self.SIGBUS}

        return signals_dictionary.get(signal, self.KILLSIGNAL)

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

    def add_error_code(self, errorcode, pilot_error_codes=[], pilot_error_diags=[], priority=False, msg=None):
        """
        Add pilot error code to list of error codes.
        This function adds the given error code to the list of all errors that have occurred. This is needed since
        several errors can happen; e.g. a stage-in error can be followed by a stage-out error during the log transfer.
        The full list of errors is dumped to the log, but only the first error is reported to the server.
        The function also sets the corresponding error message.

        :param errorcode: pilot error code (integer)
        :param pilot_error_codes: list of pilot error codes (list of integers)
        :param pilot_error_diags: list of pilot error diags (list of strings)
        :param priority: if set to True, the new errorcode will be added to the error code list first (highest priority)
        :param msg: error message (more detailed) to overwrite standard error message (string).
        :return: pilot_error_codes, pilot_error_diags
        """

        # do nothing if the error code has already been added
        if errorcode not in pilot_error_codes:
            error_msg = msg if msg else self.get_error_message(errorcode)
            if priority:
                pilot_error_codes.insert(0, errorcode)
                pilot_error_diags.insert(0, error_msg)
            else:
                pilot_error_codes.append(errorcode)
                pilot_error_diags.append(error_msg)

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
        elif exit_code == 255 and "Operation not permitted" in stderr:
            ec = self.SINGULARITYGENERALFAILURE
        elif exit_code == 64 and "Singularity is not installed" in stderr:
            ec = self.SINGULARITYNOTINSTALLED
        elif exit_code == 64 and "cannot create directory" in stderr:
            ec = self.MKDIR
        elif exit_code == -1:
            ec = self.UNKNOWNTRFFAILURE
        else:
            # do not assign a pilot error code for unidentified transform error, return 0
            ec = 0

        return ec

    def extract_stderr_error(self, stderr):
        """
        Extract the ERROR message from the payload stderr.
        :param stderr: string.
        :return: string.
        """

        # first look for special messages (ie cases not containing ERROR, Error or error labels)
        if "command not found" in stderr:
            msg = stderr
        else:
            msg = self.get_message_for_pattern([r"ERROR\s*:\s*(.*)", r"Error\s*:\s*(.*)", r"error\s*:\s*(.*)"], stderr)
        return msg

    def extract_stderr_warning(self, stderr):
        """
        Extract the WARNING message from the payload stderr.
        :param stderr: string.
        :return: string.
        """

        return self.get_message_for_pattern([r"WARNING\s*:\s*(.*)", r"Warning\s*:\s*(.*)", r"warning\s*:\s*(.*)"], stderr)

    def get_message_for_pattern(self, patterns, stderr):
        """

        :param patterns: list of patterns.
        :param stderr: string.
        :return: string.
        """

        msg = ""
        for pattern in patterns:
            found = re.findall(pattern, stderr)
            if len(found) > 0:
                msg = found[0]
                break

        return msg

    def format_diagnostics(self, code, diag):
        """
        Format the error diagnostics by adding the standard error message and the tail of the longer piloterrordiag.
        If there is any kind of failure handling the diagnostics string, the standard error description will be returned.

        :param code: standard error code (int).
        :param diag: dynamic error diagnostics (string).
        :return: formatted error diagnostics (string).
        """

        max_message_length = 256
        try:
            standard_message = self._error_messages[code] + ":"
        except Exception:
            standard_message = ""

        # extract the relevant info for reporting exceptions
        if "Traceback" in diag:
            pattern = 'details:(.+)'
            found = re.findall(pattern, diag)
            if found:
                diag = found[0]
                diag = re.sub(r'\[?PilotException\(\"?\'?', r'', diag)
                diag = re.sub(r'\[?StageInFailure\(\"?\'?', r'', diag)
                diag = re.sub(r'\[?StageOutFailure\(\"?\'?', r'', diag)
                diag = re.sub(' +', ' ', diag)

        try:
            if diag:
                # ensure that the message to be displayed on the PanDA monitor is not longer than max_message_length
                # if it is, then reformat it so that the standard message is always displayed first.
                # e.g. "Failed to stage-in file:abcdefghijklmnopqrstuvwxyz0123456789"
                if standard_message in diag:
                    if len(diag) > max_message_length:
                        error_message = standard_message + diag[-(max_message_length - len(standard_message)):]
                    else:
                        error_message = standard_message + diag[len(standard_message):][-max_message_length:]
                else:
                    if len(diag) + len(standard_message) > max_message_length:
                        error_message = standard_message + diag[:(max_message_length + len(standard_message))]
                    else:
                        error_message = standard_message + diag

                if '::' in error_message:
                    error_message = re.sub(':+', ':', error_message)

            else:
                error_message = standard_message
        except Exception:
            error_message = diag

        return error_message

    @classmethod
    def is_recoverable(self, code=0):
        """
        Determine whether code is a recoverable error code or not.

        :param code: Pilot error code (int).
        :return: boolean.
        """

        return code in self.recoverable_error_codes
