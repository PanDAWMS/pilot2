#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2021
# - Wen Guan, wen.guan@cern.ch, 2018

import time
import os
import signal
from subprocess import PIPE

from pilot.common.errorcodes import ErrorCodes
from pilot.control.job import send_state
from pilot.util.auxiliary import set_pilot_state, show_memory_usage
# from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.constants import UTILITY_BEFORE_PAYLOAD, UTILITY_WITH_PAYLOAD, UTILITY_AFTER_PAYLOAD_STARTED, \
    UTILITY_AFTER_PAYLOAD_FINISHED, PILOT_PRE_SETUP, PILOT_POST_SETUP, PILOT_PRE_PAYLOAD, PILOT_POST_PAYLOAD, \
    UTILITY_AFTER_PAYLOAD_STARTED2, UTILITY_AFTER_PAYLOAD_FINISHED2
from pilot.util.filehandling import write_file
from pilot.util.processes import kill_processes
from pilot.util.timing import add_to_pilot_timing
from pilot.common.exception import PilotException

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


class Executor(object):
    def __init__(self, args, job, out, err, traces):
        self.__args = args
        self.__job = job
        self.__out = out  # payload stdout file object
        self.__err = err  # payload stderr file object
        self.__traces = traces
        self.__preprocess_stdout_name = ''
        self.__preprocess_stderr_name = ''
        self.__coprocess_stdout_name = 'coprocess_stdout.txt'
        self.__coprocess_stderr_name = 'coprocess_stderr.txt'
        self.__postprocess_stdout_name = ''
        self.__postprocess_stderr_name = ''

    def get_job(self):
        """
        Get the job object.
        :return: job object.
        """
        return self.__job

    def pre_setup(self, job):
        """
        Functions to run pre setup
        :param job: job object
        """
        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_PRE_SETUP, time.time(), self.__args)

    def post_setup(self, job):
        """
        Functions to run post setup
        :param job: job object
        """
        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_POST_SETUP, time.time(), self.__args)

    def utility_before_payload(self, job):
        """
        Prepare commands/utilities to run before payload.
        These commands will be executed later (as eg the payload command setup is unknown at this point, which is
        needed for the preprocessing. Preprocessing is prepared here).

        REFACTOR

        :param job: job object.
        """
        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should we run any additional commands? (e.g. special monitoring commands)
        cmd_dictionary = user.get_utility_commands(order=UTILITY_BEFORE_PAYLOAD, job=job)
        if cmd_dictionary:
            cmd = '%s %s' % (cmd_dictionary.get('command'), cmd_dictionary.get('args'))
            logger.info('utility command (\'%s\') to be executed before the payload: %s', cmd_dictionary.get('label', 'utility'), cmd)

        return cmd

    def utility_with_payload(self, job):
        """
        Functions to run with payload.

        REFACTOR

        :param job: job object.
        """
        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should any additional commands be prepended to the payload execution string?
        cmd_dictionary = user.get_utility_commands(order=UTILITY_WITH_PAYLOAD, job=job)
        if cmd_dictionary:
            cmd = '%s %s' % (cmd_dictionary.get('command'), cmd_dictionary.get('args'))
            logger.info('utility command (\'%s\') to be executed with the payload: %s', cmd_dictionary.get('label', 'utility'), cmd)

        return cmd

    def get_utility_command(self, order=None):
        """
        Return the command for the requested utility command (will be downloaded if necessary).
        Note: the utility itself is defined in the user common code and is defined according to the order,
        e.g. UTILITY_AFTER_PAYLOAD_STARTED means a co-process (see ATLAS user code).

        :param order: order constant (const).
        :return: command to be executed (string).
        """

        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should any additional commands be executed after the payload?
        cmd_dictionary = user.get_utility_commands(order=order, job=self.__job)
        if cmd_dictionary:
            cmd = '%s %s' % (cmd_dictionary.get('command'), cmd_dictionary.get('args'))
            logger.info('utility command (\'%s\') to be executed after the payload: %s', cmd_dictionary.get('label', 'utility'), cmd)

        return cmd

    def utility_after_payload_started(self, job):
        """
        Functions to run after payload started
        :param job: job object
        """

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should any additional commands be executed after the payload?
        cmd_dictionary = user.get_utility_commands(order=UTILITY_AFTER_PAYLOAD_STARTED, job=job)
        if cmd_dictionary:
            cmd = '%s %s' % (cmd_dictionary.get('command'), cmd_dictionary.get('args'))
            logger.info('utility command to be executed after the payload: %s', cmd)

            # how should this command be executed?
            utilitycommand = user.get_utility_command_setup(cmd_dictionary.get('command'), job)
            if not utilitycommand:
                logger.warning('empty utility command - nothing to run')
                return
            try:
                proc1 = execute(utilitycommand, workdir=job.workdir, returnproc=True, usecontainer=False,
                                stdout=PIPE, stderr=PIPE, cwd=job.workdir, job=job)
            except Exception as error:
                logger.error('could not execute: %s', error)
            else:
                # store process handle in job object, and keep track on how many times the command has been launched
                # also store the full command in case it needs to be restarted later (by the job_monitor() thread)
                job.utilities[cmd_dictionary.get('command')] = [proc1, 1, utilitycommand]

    def utility_after_payload_started_new(self, job):
        """
        Functions to run after payload started

        REFACTOR

        :param job: job object
        """
        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should any additional commands be executed after the payload?
        cmd_dictionary = user.get_utility_commands(order=UTILITY_AFTER_PAYLOAD_STARTED, job=job)
        if cmd_dictionary:
            cmd = '%s %s' % (cmd_dictionary.get('command'), cmd_dictionary.get('args'))
            logger.info('utility command to be executed after the payload: %s', cmd)

        return cmd

#            # how should this command be executed?
#            utilitycommand = user.get_utility_command_setup(cmd_dictionary.get('command'), job)
#            if not utilitycommand:
#                logger.warning('empty utility command - nothing to run')
#                return
#            try:
#                proc = execute(utilitycommand, workdir=job.workdir, returnproc=True, usecontainer=False,
#                               stdout=PIPE, stderr=PIPE, cwd=job.workdir, job=job)
#            except Exception as error:
#                logger.error('could not execute: %s', error)
#            else:
#                # store process handle in job object, and keep track on how many times the command has been launched
#                # also store the full command in case it needs to be restarted later (by the job_monitor() thread)
#                job.utilities[cmd_dictionary.get('command')] = [proc, 1, utilitycommand]

    def utility_after_payload_finished(self, job, order):
        """
        Prepare commands/utilities to run after payload has finished.

        This command will be executed later.

        The order constant can be UTILITY_AFTER_PAYLOAD_FINISHED, UTILITY_AFTER_PAYLOAD_FINISHED2

        :param job: job object.
        :param order: constant used for utility selection (constant).
        :return: command (string), label (string).
        """

        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3

        # should any additional commands be prepended to the payload execution string?
        cmd_dictionary = user.get_utility_commands(order=order, job=job)
        if cmd_dictionary:
            cmd = '%s %s' % (cmd_dictionary.get('command'), cmd_dictionary.get('args'))
            logger.info('utility command (\'%s\') to be executed after the payload has finished: %s', cmd_dictionary.get('label', 'utility'), cmd)

        return cmd, cmd_dictionary.get('label'), cmd_dictionary.get('ignore_failure')

    def execute_utility_command(self, cmd, job, label):
        """
        Execute a utility command (e.g. pre/postprocess commands; label=preprocess etc).

        :param cmd: full command to be executed (string).
        :param job: job object.
        :param label: command label (string).
        :return: exit code (int).
        """

        exit_code, stdout, stderr = execute(cmd, workdir=job.workdir, cwd=job.workdir, usecontainer=False)
        if exit_code:
            ignored_exit_codes = [160, 161, 162]
            logger.warning('command returned non-zero exit code: %s (exit code = %d) - see utility logs for details', cmd, exit_code)
            if label == 'preprocess':
                err = errors.PREPROCESSFAILURE
            elif label == 'postprocess':
                err = errors.POSTPROCESSFAILURE
            else:
                err = 0  # ie ignore
                exit_code = 0
            if err and exit_code not in ignored_exit_codes:  # ignore no-more-data-points exit codes
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(err)
            if exit_code in ignored_exit_codes:
                job.transexitcode = exit_code

        # write output to log files
        self.write_utility_output(job.workdir, label, stdout, stderr)

        return exit_code

    def write_utility_output(self, workdir, step, stdout, stderr):
        """
        Write the utility command output to stdout, stderr files to the job.workdir for the current step.
        -> <step>_stdout.txt, <step>_stderr.txt
        Example of step: preprocess, postprocess.

        :param workdir: job workdir (string).
        :param step: utility step (string).
        :param stdout: command stdout (string).
        :param stderr: command stderr (string).
        :return:
        """

        # dump to file
        try:
            name_stdout = step + '_stdout.txt'
            name_stderr = step + '_stderr.txt'
            if step == 'preprocess':
                self.__preprocess_stdout_name = name_stdout
                self.__preprocess_stderr_name = name_stderr
            elif step == 'postprocess':
                self.__postprocess_stdout_name = name_stdout
                self.__postprocess_stderr_name = name_stderr
            name = os.path.join(workdir, step + '_stdout.txt')
            write_file(name, stdout, unique=True)
        except PilotException as error:
            logger.warning('failed to write utility stdout to file: %s, %s', error, stdout)
        else:
            logger.debug('wrote %s', name)

        try:
            name = os.path.join(workdir, step + '_stderr.txt')
            write_file(name, stderr, unique=True)
        except PilotException as error:
            logger.warning('failed to write utility stderr to file: %s, %s', error, stderr)
        else:
            logger.debug('wrote %s', name)

    def pre_payload(self, job):
        """
        Calls to functions to run before payload.
        E.g. write time stamps to timing file.

        :param job: job object.
        """
        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_PRE_PAYLOAD, time.time(), self.__args)

    def post_payload(self, job):
        """
        Calls to functions to run after payload.
        E.g. write time stamps to timing file.

        :param job: job object
        """
        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_POST_PAYLOAD, time.time(), self.__args)

    def run_command(self, cmd, label=None):
        """
        Execute the given command and return the process id.

        :param cmd: command (string).
        :return: process id (int).
        """

        if label:
            logger.info('\n\n%s:\n\n%s\n', label, cmd)
        if label == 'coprocess':
            try:
                out = open(os.path.join(self.__job.workdir, self.__coprocess_stdout_name), 'wb')
                err = open(os.path.join(self.__job.workdir, self.__coprocess_stderr_name), 'wb')
            except Exception as error:
                logger.warning('failed to open coprocess stdout/err: %s', error)
                out = None
                err = None
        else:
            out = None
            err = None
        try:
            proc = execute(cmd, workdir=self.__job.workdir, returnproc=True, stdout=out, stderr=err,
                           usecontainer=False, cwd=self.__job.workdir, job=self.__job)
        except Exception as error:
            logger.error('could not execute: %s', error)
            return None
        if isinstance(proc, tuple) and not proc[0]:
            logger.error('failed to execute command')
            return None

        logger.info('started %s -- pid=%s executable=%s', label, proc.pid, cmd)

        return proc

    def run_payload(self, job, cmd, out, err):
        """
        Setup and execute the main payload process.

        REFACTOR using run_command()

        :param job: job object.
        :param out: (currently not used; deprecated)
        :param err: (currently not used; deprecated)
        :return: proc (subprocess returned by Popen())
        """

        # main payload process steps

        # add time for PILOT_PRE_PAYLOAD
        self.pre_payload(job)

        logger.info("\n\npayload execution command:\n\n%s\n", cmd)
        try:
            proc = execute(cmd, workdir=job.workdir, returnproc=True,
                           usecontainer=True, stdout=out, stderr=err, cwd=job.workdir, job=job)
        except Exception as error:
            logger.error('could not execute: %s', error)
            return None
        if isinstance(proc, tuple) and not proc[0]:
            logger.error('failed to execute payload')
            return None

        logger.info('started -- pid=%s executable=%s', proc.pid, cmd)
        job.pid = proc.pid
        job.pgrp = os.getpgid(job.pid)
        set_pilot_state(job=job, state="running")

        #_cmd = self.utility_with_payload(job)

        self.utility_after_payload_started(job)

        return proc

    def extract_setup(self, cmd):
        """
        Extract the setup from the payload command (cmd).
        E.g. extract the full setup from the payload command will be prepended to the pre/postprocess command.

        :param cmd: payload command (string).
        :return: updated secondary command (string).
        """

        def cut_str_from(_cmd, _str):
            """
            Cut the string from the position of the given _cmd
            """
            return _cmd[:_cmd.find(_str)]

        def cut_str_from_last_semicolon(_cmd):
            """
            Cut the string from the last semicolon
            NOTE: this will not work if jobParams also contain ;
            """
            # remove any trailing spaces and ;-signs
            _cmd = _cmd.strip()
            _cmd = _cmd[:-1] if _cmd.endswith(';') else _cmd
            last_bit = _cmd.split(';')[-1]
            return _cmd.replace(last_bit.strip(), '')

        if '/' in self.__job.transformation:  # e.g. http://pandaserver.cern.ch:25080/trf/user/runHPO-00-00-01
            trfname = self.__job.transformation[self.__job.transformation.rfind('/') + 1:]  # runHPO-00-00-01
            _trf = './' + trfname
        else:
            trfname = self.__job.transformation
            _trf = './' + self.__job.transformation

        if _trf in cmd:
            setup = cut_str_from(cmd, _trf)
        elif trfname in cmd:
            setup = cut_str_from(cmd, trfname)
        else:
            setup = cut_str_from_last_semicolon(cmd)

        return setup

    def wait_graceful(self, args, proc):
        """
        Wait for payload process to finish.

        :param args: Pilot arguments object.
        :param proc: Process id (int).
        :return: exit code (int).
        """

        breaker = False
        exit_code = None
        try:
            iteration = long(0)  # Python 2, do not use 0L since it will create a syntax error in spite of the try # noqa: F821
        except Exception:
            iteration = 0  # Python 3, long doesn't exist
        while True:
            time.sleep(0.1)

            iteration += 1
            for _ in range(60):  # Python 2/3
                if args.graceful_stop.is_set():
                    breaker = True
                    logger.info('breaking -- sending SIGTERM pid=%s', proc.pid)
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    break
                exit_code = proc.poll()
                if exit_code is not None:
                    break
                time.sleep(1)
            if breaker:
                logger.info('breaking -- sleep 3s before sending SIGKILL pid=%s', proc.pid)
                time.sleep(3)
                proc.kill()
                break

            exit_code = proc.poll()

            if iteration % 10 == 0:
                logger.info('running: iteration=%d pid=%s exit_code=%s', iteration, proc.pid, exit_code)
            if exit_code is not None:
                break
            else:
                continue

        return exit_code

    def get_payload_command(self, job):
        """
        Return the payload command string.

        :param job: job object.
        :return: command (string).
        """

        cmd = ""
        # for testing looping job: cmd = user.get_payload_command(job) + ';sleep 240'
        try:
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user],
                              0)  # Python 2/3
            cmd = user.get_payload_command(job)  #+ 'sleep 1000'  # to test looping jobs
        except PilotException as error:
            self.post_setup(job)
            import traceback
            logger.error(traceback.format_exc())
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())
            self.__traces.pilot['error_code'] = job.piloterrorcodes[0]
            logger.fatal(
                'could not define payload command (traces error set to: %d)', self.__traces.pilot['error_code'])

        return cmd

    def run_preprocess(self, job):
        """
        Run any preprocess payloads.

        :param job: job object.
        :return:
        """

        exit_code = 0

        try:
            # note: this might update the jobparams
            cmd_before_payload = self.utility_before_payload(job)
        except Exception as error:
            logger.error(error)
            raise error

        if cmd_before_payload:
            cmd_before_payload = job.setup + cmd_before_payload
            logger.info("\n\npreprocess execution command:\n\n%s\n", cmd_before_payload)
            exit_code = self.execute_utility_command(cmd_before_payload, job, 'preprocess')
            if exit_code == 160:
                logger.warning('no more HP points - time to abort processing loop')
            elif exit_code == 161:
                logger.warning('no more HP points but at least one point was processed - time to abort processing loop')
            elif exit_code == 162:
                logger.warning('loop count reached the limit - time to abort processing loop')
            elif exit_code:
                # set error code
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PREPROCESSFAILURE)
                logger.fatal('cannot continue since preprocess failed: exit_code=%d', exit_code)
            else:
                # in case the preprocess produced a command, chmod it
                path = os.path.join(job.workdir, job.containeroptions.get('containerExec', 'does_not_exist'))
                if os.path.exists(path):
                    logger.debug('chmod 0o755: %s', path)
                    os.chmod(path, 0o755)

        return exit_code

    def run(self):  # noqa: C901
        """
        Run all payload processes (including pre- and post-processes, and utilities).
        In the case of HPO jobs, this function will loop over all processes until the preprocess returns a special
        exit code.
        :return:
        """

        # get the payload command from the user specific code
        self.pre_setup(self.__job)

        cmd = self.get_payload_command(self.__job)
        # extract the setup in case the preprocess command needs it
        self.__job.setup = self.extract_setup(cmd)
        self.post_setup(self.__job)

        # a loop is needed for HPO jobs
        # abort when nothing more to run, or when the preprocess returns a special exit code
        iteration = 0
        while True:

            logger.info('payload iteration loop #%d', iteration + 1)
            os.environ['PILOT_EXEC_ITERATION_COUNT'] = '%s' % iteration
            show_memory_usage()

            # first run the preprocess (if necessary) - note: this might update jobparams -> must update cmd
            jobparams_pre = self.__job.jobparams
            exit_code = self.run_preprocess(self.__job)
            jobparams_post = self.__job.jobparams
            if exit_code:
                if exit_code >= 160 and exit_code <= 162:
                    exit_code = 0
                    # wipe the output file list since there won't be any new files
                    # any output files from previous iterations, should have been transferred already
                    logger.debug('reset outdata since further output should not be expected after preprocess exit')
                    self.__job.outdata = []
                break
            if jobparams_pre != jobparams_post:
                logger.debug('jobparams were updated by utility_before_payload()')
                # must update cmd
                cmd = cmd.replace(jobparams_pre, jobparams_post)

            # now run the main payload, when it finishes, run the postprocess (if necessary)
            # note: no need to run any main payload in HPO Horovod jobs on Kubernetes
            if os.environ.get('HARVESTER_HOROVOD', '') == '':

                #exit_code, _stdout, _stderr = execute('pgrep -x xrootd | awk \'{print \"ps -p \"$1\" -o args --no-headers --cols 300\"}\' | sh')
                #logger.debug('[before payload start] stdout=%s', _stdout)
                #logger.debug('[before payload start] stderr=%s', _stderr)

                proc = self.run_payload(self.__job, cmd, self.__out, self.__err)
            else:
                proc = None

            proc_co = None
            if proc is None:
                # run the post-process command even if there was no main payload
                if os.environ.get('HARVESTER_HOROVOD', '') != '':
                    logger.info('No need to execute any main payload')
                    exit_code = self.run_utility_after_payload_finished(exit_code, True, UTILITY_AFTER_PAYLOAD_FINISHED2)
                    self.post_payload(self.__job)
                else:
                    break
            else:
                # the process is now running, update the server
                # test 'tobekilled' from here to try payload kill
                send_state(self.__job, self.__args, self.__job.state)

                # note: when sending a state change to the server, the server might respond with 'tobekilled'
                if self.__job.state == 'failed':
                    logger.warning('job state is \'failed\' - abort payload and run()')
                    kill_processes(proc.pid)
                    break

                # allow for a secondary command to be started after the payload (e.g. a coprocess)
                utility_cmd = self.get_utility_command(order=UTILITY_AFTER_PAYLOAD_STARTED2)
                if utility_cmd:
                    logger.debug('starting utility command: %s', utility_cmd)
                    label = 'coprocess' if 'coprocess' in utility_cmd else None
                    proc_co = self.run_command(utility_cmd, label=label)

                logger.info('will wait for graceful exit')
                exit_code = self.wait_graceful(self.__args, proc)
                # reset error if Raythena decided to kill payload (no error)
                if errors.KILLPAYLOAD in self.__job.piloterrorcodes:
                    logger.debug('ignoring KILLPAYLOAD error')
                    self.__job.piloterrorcodes, self.__job.piloterrordiags = errors.remove_error_code(errors.KILLPAYLOAD,
                                                                                                      pilot_error_codes=self.__job.piloterrorcodes,
                                                                                                      pilot_error_diags=self.__job.piloterrordiags)
                    exit_code = 0
                    state = 'finished'
                else:
                    state = 'finished' if exit_code == 0 else 'failed'
                set_pilot_state(job=self.__job, state=state)
                logger.info('\n\nfinished pid=%s exit_code=%s state=%s\n', proc.pid, exit_code, self.__job.state)

                #exit_code, _stdout, _stderr = execute('pgrep -x xrootd | awk \'{print \"ps -p \"$1\" -o args --no-headers --cols 300\"}\' | sh')
                #logger.debug('[after payload finish] stdout=%s', _stdout)
                #logger.debug('[after payload finish] stderr=%s', _stderr)

                # stop the utility command (e.g. a coprocess if necessary
                if proc_co:
                    logger.debug('stopping utility command: %s', utility_cmd)
                    kill_processes(proc_co.pid)

                if exit_code is None:
                    logger.warning('detected unset exit_code from wait_graceful - reset to -1')
                    exit_code = -1

                for order in [UTILITY_AFTER_PAYLOAD_FINISHED, UTILITY_AFTER_PAYLOAD_FINISHED2]:
                    exit_code = self.run_utility_after_payload_finished(exit_code, state, order)

                self.post_payload(self.__job)

                # stop any running utilities
                if self.__job.utilities != {}:
                    self.stop_utilities()

            if self.__job.is_hpo and state != 'failed':
                # in case there are more hyper-parameter points, move away the previous log files
                #self.rename_log_files(iteration)
                iteration += 1
            else:
                break

        return exit_code

    def run_utility_after_payload_finished(self, exit_code, state, order):
        """
        Run utility command after the main payload has finished.
        In horovod mode, select the corresponding post-process. Otherwise, select different post-process (e.g. Xcache).

        The order constant can be UTILITY_AFTER_PAYLOAD_FINISHED, UTILITY_AFTER_PAYLOAD_FINISHED2

        :param exit_code: transform exit code (int).
        :param state: payload state; finished/failed (string).
        :param order: constant used for utility selection (constant).
        :return: exit code (int).
        """

        _exit_code = 0
        try:
            cmd_after_payload, label, ignore_failure = self.utility_after_payload_finished(self.__job, order)
        except Exception as error:
            logger.error(error)
            ignore_failure = False
        else:
            if cmd_after_payload and self.__job.postprocess and state != 'failed':
                cmd_after_payload = self.__job.setup + cmd_after_payload
                logger.info("\n\npostprocess execution command:\n\n%s\n", cmd_after_payload)
                _exit_code = self.execute_utility_command(cmd_after_payload, self.__job, label)
            elif cmd_after_payload:
                logger.info("\n\npostprocess execution command:\n\n%s\n", cmd_after_payload)
                _exit_code = self.execute_utility_command(cmd_after_payload, self.__job, label)

        # only set a new non-zero exit code if exit_code was not already set and ignore_failure is False
        # (e.g. any Xcache failure should be ignored to prevent job from failing since exit_code might get updated)
        if _exit_code and not exit_code and not ignore_failure:
            exit_code = _exit_code

        return exit_code

    def stop_utilities(self):
        """
        Stop any running utilities.

        :return:
        """

        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()

        for utcmd in list(self.__job.utilities.keys()):  # Python 2/3
            utproc = self.__job.utilities[utcmd][0]
            if utproc:
                user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)  # Python 2/3
                sig = user.get_utility_command_kill_signal(utcmd)
                logger.info("stopping process \'%s\' with signal %d", utcmd, sig)
                try:
                    os.killpg(os.getpgid(utproc.pid), sig)
                except Exception as error:
                    logger.warning('exception caught: %s (ignoring)', error)

                user.post_utility_command_action(utcmd, self.__job)

    def rename_log_files(self, iteration):
        """

        :param iteration:
        :return:
        """

        names = [self.__preprocess_stdout_name, self.__preprocess_stderr_name,
                 self.__postprocess_stdout_name, self.__postprocess_stderr_name]
        for name in names:
            if os.path.exists(name):
                os.rename(name, name + '%d' % iteration)
            else:
                logger.warning('cannot rename %s since it does not exist', name)
