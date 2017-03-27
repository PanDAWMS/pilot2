#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

"""
Module defining the Signal class.
"""

import inspect
import threading
from weakref import WeakSet, WeakKeyDictionary
import logging
from exception_formatter import caught
from functools import wraps
import sys

DEBUG = True


class SignalDispatcher(threading.Thread):
    """
    A thread to dispatch a signal if it is emitted in asynchronous mode.

    Like in `async` decorator, here the thread gets the info about it's parent thread and logs it up.
    """
    def __init__(self, sig, args, kwargs):
        """
        Sets up necessary parameters like thread name, it's parent's name and id, emitter...
        :param (Signal) sig: signal reference
        :param args: arguments of the signal call
        :param kwargs: KV arguments of the signal call
        """
        super(SignalDispatcher, self).__init__(name=sig.name)
        self.dispatch_async_signal = sig
        self.args = args
        self.kwargs = kwargs

        self.emitter = sig.emitter
        current = threading.currentThread()
        self.parent = (current.getName(), current.ident)

    def run(self):
        """
        Thread entry point.
        Logs the thread ancestry.
        Starts the signal.
        Logs exceptions if necessary.
        """
        logging.debug("Thread: %s(%d), called from: %s(%d)" % (self.getName(), self.ident,
                                                               self.parent[0], self.parent[1]))
        try:
            self.dispatch_async_signal._just_call(*self.args, **self.kwargs)
        except Exception as e:
            caught(e, sys.exc_info())


def slot_fix(method):
    @wraps(method)
    def wrapper(self, slot):
        if isinstance(slot, threading.Event):
            slot = slot.set
        return method(self, slot)

    return wrapper


class Signal(object):
    """
    This class provides Signal-Slot pattern from Qt to python.

    To create a signal, just make a `sig = Signal` and set up an emitter of it. Or create it with
    `sig = Signal(emitter=foo)`.

    To emit it, just call your `sig()`.
    Or emit it in asynchronous mode: `sig.async()`.

    To connect slots to it, pass callbacks into `sig.connect`. The connections are maintained through weakrefs, thus
    you don't need to search for them and disconnect whenever you're up to destroy some object.

    TODO: Events
    """
    name = "BasicSignal"

    def __init__(self, emitter=None, docstring=None):
        """
        Creates a Signal class with no connections.

        :param emitter: Any object or anything, that is bound to a signal
        :param (basestring) docstring: if necessary, you may provide a docstring for this signal instead of the default
                                       one.
        """
        self._functions = WeakSet()
        self._methods = WeakKeyDictionary()
        self._slots_lk = threading.RLock()
        self.emitter = emitter  # TODO: Make this weakref
        if isinstance(docstring, basestring):
            self.__doc__ = docstring

    @slot_fix
    def connect(self, slot):
        """
        Connect a callback ``slot`` to this signal if it is not connected already.
        """
        with self._slots_lk:
            if not self.is_connected(slot):
                if inspect.ismethod(slot):
                    if slot.im_self not in self._methods:
                        self._methods[slot.im_self] = set()

                    self._methods[slot.im_self].add(slot.im_func)

                else:
                    self._functions.add(slot)

    @slot_fix
    def is_connected(self, slot):
        """
        Check if a callback ``slot`` is connected to this signal.
        """
        with self._slots_lk:
            if inspect.ismethod(slot):
                if slot.im_self in self._methods and slot.im_func in self._methods[slot.im_self]:
                    return True
                return False
            return slot in self._functions

    @slot_fix
    def disconnect(self, slot):
        """
        Disconnect a ``slot`` from a signal if it is connected else do nothing.
        """
        with self._slots_lk:
            if self.is_connected(slot):
                if inspect.ismethod(slot):
                    self._methods[slot.im_self].remove(slot.im_func)
                else:
                    self._functions.remove(slot)

    @staticmethod
    def emitted():
        """
        As the signal may provide emitter and other stuff related, this function gets the signal that was emitted.

        Note! Uses inspect.

        :return Signal:
        """
        frame = inspect.currentframe()
        outer = inspect.getouterframes(frame)
        self = None  # type: Signal
        for i in outer:
            if 'self' in i[0].f_locals and isinstance(i[0].f_locals['self'], Signal):
                self = i[0].f_locals['self']
                break

        del frame
        del outer
        return self

    def debug_frame_message(self):
        """
        Outputs a name and a line on which a signal was caught. Debug info.
        """
        if not DEBUG:
            return
        logger = logging.getLogger("Signal")
        frame = inspect.currentframe()
        outer = inspect.getouterframes(frame)
        signal_frame = outer[2]
        try:
            logger.handle(logger.makeRecord(logger.name, logging.DEBUG, signal_frame[1], signal_frame[2],
                                            signal_frame[4][0].strip() + " -> " + self.name, (), None, "emit"))
        finally:
            del signal_frame
            del outer
            del frame

    def async(self, *args, **kwargs):
        """
        Emits the signal in the asynchronous mode. Arguments are passed to the callbacks.

        Variadic.
        :return SignalDispatcher:
        """
        self.debug_frame_message()
        sd = SignalDispatcher(self, args, kwargs)
        sd.start()
        return sd

    def __call__(self, *args, **kwargs):
        """
        Emits the signal in the synchronous mode. Arguments are passed to the callbacks.

        Variadic, Reentrant.
        """
        self.debug_frame_message()
        self._just_call(*args, **kwargs)
        return self

    def _just_call(self, *args, **kwargs):
        with self._slots_lk:
            # Call handler functions
            for func in self._functions:
                func(*args, **kwargs)

            # Call handler methods
            for obj, funcs in self._methods.items():
                for func in funcs:
                    func(obj, *args, **kwargs)
