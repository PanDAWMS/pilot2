#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

import signal
import os
import inspect
from signalslot import Signal
import threading

_is_set_up = False

signals_reverse = {}
"""
These hold names of the signals in the respect to their numbers. Useful for logs.
"""

graceful_terminator = signal.SIGTERM
"""
When on UNIX and others, just a SIGTERM, but on Windows -- a CTRL_BREAK_EVENT.
Can be used to inform a child of the graceful shutdown or other stuff.
"""

_receiver = Signal(signal, docstring="""
This signal (from signal/slot pattern) will serve the signal (from OS) for it's listeners.
""")
_receiver.name = "OS Signal dispatcher"

if os.name == "nt":
    """
    The biggest problem of all the systems is the Windows support.
    Here comes the Ctri+C and Ctrl+Break signal handler, and override of the graceful terminator.
    """
    graceful_terminator = signal.CTRL_BREAK_EVENT

    import ctypes
    from ctypes import wintypes

    _kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)

    def _check_bool(result, func, args):
        """
        Don't ask me about this magic, just copied from stackoverflow and it works.
        """
        if not result:
            raise ctypes.WinError(ctypes.get_last_error())
        # else build final result from result, args, outmask, and
        # inoutmask. Typically it's just result, unless you specify
        # out/inout parameters in the prototype.
        return args

    _HandlerRoutine = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)

    _kernel32.SetConsoleCtrlHandler.errcheck = _check_bool
    _kernel32.SetConsoleCtrlHandler.argtypes = (_HandlerRoutine,
                                                wintypes.BOOL)

    _console_ctrl_handlers = {}

    def set_console_ctrl_handler(handler):
        """
        Don't ask me about this magic, just copied from stackoverflow and it works.
        """
        if handler not in _console_ctrl_handlers:
            h = _HandlerRoutine(handler)
            _kernel32.SetConsoleCtrlHandler(h, True)
            _console_ctrl_handlers[handler] = h

    signals_reverse[signal.CTRL_C_EVENT] = 'CTRL_C_EVENT'
    signals_reverse[signal.CTRL_BREAK_EVENT] = 'CTRL_BREAK_EVENT'

    _receiver.emitter = _kernel32

    def handler(sig):
        """
        This function creates an inspection frame for windows handlers.
        :param (int) sig: caught signal.
        :return:
        """
        simulate_signal(sig)
        return 1


def simulate_signal(sig=graceful_terminator):
    """
    This function simulates signal calling.
    It creates an inspection frame and passes it to receivers.
    :param (int) sig: caught signal, default is `graceful_terminator`
    """
    frame = inspect.currentframe()
    try:
        _receiver(sig, frame)
    finally:
        del frame


def graceful_stop_event():
    """
    As threading.Event, this is just a factory.
    """
    ret = threading.Event()
    signal_all_setup(ret)
    return ret


def signal_all_setup(func=None):
    """
    Tries to establish catching all the signals possible and adds the callback to a _receiver.
    Also fills up all signals for a reverse lookup.

    Signal dispatcher is called synchronously to prevent any var rushes if the program is not capable of async calls.

    :param (Callable) func: function to call on a signal.
    """
    global _is_set_up

    if func is not None:
        _receiver.connect(func)

    if not _is_set_up:
        _is_set_up = True
        if os.name == 'nt':
            set_console_ctrl_handler(handler)
        for i in [
            'SIGINT', 'SIGHUP', 'SIGTERM', 'SIGUSR1', 'SIGUSR2', 'SIGFPE',
            'SIGQUIT', 'SIGSEGV', 'SIGXCPU', 'SIGBUS', 'SIGKILL', 'SIGILL', 'SIGBREAK'
        ]:
            if hasattr(signal, i):
                try:
                    signal.signal(getattr(signal, i), _receiver.__call__)
                    signals_reverse[getattr(signal, i)] = i
                    # print "set "+i
                except (ValueError, RuntimeError):
                    # print "error with "+i
                    pass
