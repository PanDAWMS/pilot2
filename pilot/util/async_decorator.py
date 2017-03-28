#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017

import threading
from exception_formatter import caught
from functools import wraps
import logging
import sys


class TimeoutError(RuntimeError):
    """
    Raised during wait, if timeout reached.
    """
    pass


class AsyncCall(threading.Thread):
    """
    This is a threading wrapper that performs asynchronous call of the provided function.

    Basic usage:
    ```python
    thread = AsyncCall(func)(...args)
    result = thread.wait()
    #  or
    result = AsyncCall(func)(...args).wait()
    ```
    """
    Result = None
    args = []
    kwargs = {}

    def __init__(self, function, callback=None, daemon=False):
        """
        Saves function, it's callback and daemon state.

        :param (Callable) function: The function to call.
        :param (Callable) callback: The callback, if needed, should be capable of holding the return of the function.
        :param (Boolean) daemon: Whether the thread is a daemon. Daemon threads die automatically when no other threads
                                 left.
        """
        super(AsyncCall, self).__init__()
        self.Callable = function
        self.Callback = callback
        self.daemon = daemon

    def __call__(self, *args, **kwargs):
        """
        Thread starter.

        Saves the state, the function arguments, the thread name and starts the thread.

        Variadic, reentrant.
        """
        func = self.Callable
        self.name = "%s:%d:%s" % (func.func_globals["__name__"], func.func_code.co_firstlineno, func.__name__)

        current = threading.currentThread()
        self.parent = (current.getName(), current.ident)

        self.args = args
        self.kwargs = kwargs
        self.start()
        return self

    def wait(self, timeout=None):
        """
        Waits for the function to complete.

        :raises TimeoutError: when timeout reached.

        :param timeout: seconds, optional.
        :return:
        """
        self.join(timeout)
        if self.isAlive():
            raise TimeoutError()
        else:
            return self.Result

    def run(self):
        """
        Thread entrance point.

        Runs the function and then the callback.
        """
        logging.debug("Thread: %s(%d), called from: %s(%d)" % (self.getName(), self.ident,
                                                               self.parent[0], self.parent[1]))
        try:
            self.Result = self.Callable(*self.args, **self.kwargs)
            if self.Callback:
                self.Callback(self.Result)
        except Exception as e:
            caught(e, sys.exc_info())


def async(function=None, callback=None, daemon=False):
    """
    Decorator around the functions for them to be asynchronous.

    Used as a plain decorator or along with named arguments.

    Usage:
    ```python
    @async
    def f1():
        pass

    @async(callback=f2_callback, daemon=True)
    def f2():
        pass
    ```

    :param (Callable) function: The function to be decorated.
    :param (Callable) callback: Optional. A callback function to parse the result.
    :param (Boolean) daemon: Optional. Create the daemon thread, that will die when no other threads left.
    :return Callable: Wrapped function.
    """
    if function is None:
        def add_async_callback(func):
            """
            A second stage of a wrapper that is used if a wrapper is called with arguments.

            :param (Callable) func: The function to be decorated.
            :return Callable: Wrapped function.
            """
            return async(func, callback, daemon)
        return add_async_callback
    else:
        @wraps(function)
        def async_caller(*args, **kwargs):
            """
            An actual wrapper, that creates a thread.

            :return AsyncCall: Thread of the function.
            """
            return AsyncCall(function, callback, daemon)(*args, **kwargs)
        return async_caller
