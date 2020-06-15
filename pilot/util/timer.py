#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019

"""
Standalone implementation of time-out check on function call.
Timer stops execution of wrapped function if it reaches the limit of provided time. Supports decorator feature.

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: March 2018
"""

from __future__ import print_function  # Python 2 (2to3 complains about this)

import os
import signal
import sys

import traceback
import threading
import multiprocessing

try:
    from queue import Empty  # Python 3
except Exception:
    from Queue import Empty  # Python 2
from functools import wraps


# move this class to exception.py
class TimeoutException(Exception):

    def __init__(self, message, timeout=None, *args):
        self.timeout = timeout
        self.message = message
        self._errorCode = 1334
        super(TimeoutException, self).__init__(*args)

    def __str__(self):
        return "%s: %s, timeout=%s seconds%s" % (self.__class__.__name__, self.message, self.timeout, ' : %s' % repr(self.args) if self.args else '')


class TimedThread(object):
    """
        Thread-based Timer implementation (`threading` module)
        (shared memory space, GIL limitations, no way to kill thread, Windows compatible)
    """

    def __init__(self, timeout):
        """
            :param timeout: timeout value for operation in seconds.
        """

        self.timeout = timeout
        self.is_timeout = False

    def execute(self, func, args, kwargs):

        try:
            ret = (True, func(*args, **kwargs))
        except Exception:
            ret = (False, sys.exc_info())

        self.result = ret

        return ret

    def run(self, func, args, kwargs, timeout=None):
        """
            :raise: TimeoutException if timeout value is reached before function finished
        """

        thread = threading.Thread(target=self.execute, args=(func, args, kwargs))
        thread.daemon = True

        thread.start()

        timeout = timeout if timeout is not None else self.timeout

        thread.join(timeout)

        if thread.is_alive():
            self.is_timeout = True
            raise TimeoutException("Timeout reached", timeout=timeout)

        ret = self.result

        if ret[0]:
            return ret[1]
        else:
            try:
                _r = ret[1][0](ret[1][1]).with_traceback(ret[1][2])  # python3
            except AttributeError:
                exec("raise ret[1][0], ret[1][1], ret[1][2]")   # python3 compatible code for python2 execution
            raise _r


class TimedProcess(object):
    """
        Process-based Timer implementation (`multiprocessing` module). Uses shared Queue to keep result.
        (completely isolated memory space)
        In default python implementation multiprocessing considers (c)pickle as serialization backend
        which is not able properly (requires a hack) to pickle local and decorated functions (affects Windows only)
        Traceback data is printed to stderr
    """

    def __init__(self, timeout):
        """
            :param timeout: timeout value for operation in seconds.
        """

        self.timeout = timeout
        self.is_timeout = False

    def run(self, func, args, kwargs, timeout=None):

        def _execute(func, args, kwargs, queue):  ## will fail on Windows
            try:
                ret = func(*args, **kwargs)
                queue.put((True, ret))
            except Exception as e:
                print('Exception occurred while executing %s' % func, file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                queue.put((False, e))

        queue = multiprocessing.Queue(1)
        process = multiprocessing.Process(target=_execute, args=(func, args, kwargs, queue))
        process.daemon = True
        process.start()

        timeout = timeout if timeout is not None else self.timeout

        try:
            ret = queue.get(block=True, timeout=timeout)
        except Empty:
            self.is_timeout = True
            process.terminate()
            raise TimeoutException("Timeout reached", timeout=timeout)
        finally:
            while process.is_alive():
                process.join(1)
                if process.is_alive():  ## still alive, force terminate
                    process.terminate()
                    process.join(1)
                if process.is_alive() and process.pid:  ## still alive, hard kill
                    os.kill(process.pid, signal.SIGKILL)

            multiprocessing.active_children()

        if ret[0]:
            return ret[1]
        else:
            raise ret[1]


if getattr(os, 'fork', None):
    Timer = TimedProcess
else:  ## Windows
    Timer = TimedThread


def timeout(seconds, timer=None):
    """
    Decorator for a function which causes it to timeout (stop execution) once passed given number of seconds.
    :param timer: timer class (by default is Timer)
    :raise: TimeoutException in case of timeout interrupt
    """

    timer = timer or Timer

    def decorate(function):

        @wraps(function)
        def wrapper(*args, **kwargs):
            return timer(seconds).run(function, args, kwargs)

        return wrapper

    return decorate
