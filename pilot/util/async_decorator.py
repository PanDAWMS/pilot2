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


class Promise(threading.Thread):
    """
    This is a threading wrapper that performs asynchronous call of the provided function.

    Basic usage:
    ```python
    promise = Promise(func)(...args)
    result = promise.wait()
    #  or
    result = Promise(func)(...args).wait()

    #  callbacks
    def callback(result):
        print result
    def exception_catcher(exception):
        log(exception)
    def some_final(_):
        cleanup()

    Promise(func)(...args).then(callback, exception_catcher).then(some_final)
    ```
    For advanced usage, see https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Global_Objects/Promise
    It is close to that document, though functions in Promises can use any arguments and should be started manually.
    Rejections thus are based on exceptions and resolutions are simply function results.
    """
    Callable = None
    Result = None
    args = None
    kwargs = None
    resolved = None
    exception = None
    started = None
    print_exception = None

    def __init__(self, function=None, daemon=False, print_exception=logging.ERROR):
        """
        Saves function, it's callback and daemon state.

        :param (Callable) function: The function to call.
        :param (Boolean) daemon: Whether the thread is a daemon. Daemon threads die automatically when no other threads
                                 left.
        :param print_exception: Log level to output the exception, or None to mute it.
        """
        super(Promise, self).__init__()
        self.print_exception = print_exception
        self.Callable = function
        self.daemon = daemon
        self.__resolve_name()
        self.started = threading.Event()
        self.finished = threading.Event()

        self.__check_callable()

    def __check_callable(self):
        """
        Checks what to do with Callable:
         1) if it is not callable, treat it as a result.
         2) if it's Promise, depend on it.
        Else, pass on, wait for start.
        """
        if isinstance(self.Callable, Promise):
            pr = self.Callable

            def wait_and_resolve():
                """
                Waits for passed in Promise and resolves in the same way.
                :return:
                """
                pr.wait()
                if pr.resolved is True:
                    return pr.Result
                else:
                    raise pr.exception

            self.Callable = wait_and_resolve
            self()

        elif not callable(self.Callable):
            self.Result = self.Callable
            self.started.set()
            self.finished.set()
            self.resolved = True

    @staticmethod
    def resolve(thing):
        """
        Creates a Promise resolved to `thing`.
        :param thing:
        :return: Promise
        """
        p = Promise()
        p.Result = thing
        return p

    @staticmethod
    def reject(thing):
        """
        Creates a Promise rejected to `thing`.
        :param thing:
        :return: Promise
        """
        p = Promise()
        p.exception = thing
        p.resolved = False
        return p

    @staticmethod
    def all(things):
        """
        Creates a Promise that waits to each one of `things`, or rejects with the first one of them.
        :param (Array) things:
        :return: Promise
        """
        def all_resolver():
            """
            Internal, resolver for the new Promise.
            :return: results array
            """
            stop = threading.Event()
            stop.rejected = None
            stop.count = 0
            for i, thing in enumerate(things):
                if isinstance(thing, Promise):
                    stop.count += 1

                    def closure(i, thing):
                        """
                        Closure to save indexes.
                        :param i: index
                        :param thing: current Promise
                        :return:
                        """
                        def resolver(result):
                            things[i] = result
                            stop.set()
                            stop.count -= 1

                        def excepter(_):
                            stop.rejected = thing
                            stop.set()

                        thing.then(resolver, excepter)

                    closure(i, thing)

            while stop.count:
                stop.wait()
                stop.clear()
                if stop.rejected is not None:
                    raise stop.rejected.exception

            return things

        p = Promise(all_resolver)
        return p()

    @staticmethod
    def race(things):
        """
        Creates a Promise that waits to any of `things` and returns it's result.
        :param (Array) things:
        :return: Promise
        """
        def all_resolver():
            """
            Internal, resolver for the new Promise.
            :return: result
            """
            stop = threading.Event()
            stop.finished = None
            for thing in things:
                if isinstance(thing, Promise):
                    def closure(thing):
                        def resolver(_):
                            stop.set()
                            stop.finished = thing

                        thing.then(resolver, resolver)

                    closure(thing)
                else:
                    return thing

            stop.wait()
            if stop.finished.resolved is True:
                return stop.finished.Result
            else:
                raise stop.finished.exception

        p = Promise(all_resolver)
        return p()

    def __resolve_name(self):
        """
        Resolves the name of the Promise.
        """
        func = self.Callable
        if hasattr(func, 'func_globals') and hasattr(func, 'func_code'):
            self.name = "%s:%d:%s" % (func.func_globals["__name__"], func.func_code.co_firstlineno, func.__name__)
        else:
            self.name = 'Promise(%s)' % str(func)

    def __call__(self, *args, **kwargs):
        """
        Thread starter.

        Saves the state, the function arguments, the thread name and starts the thread.

        Variadic, reentrant.
        """
        if not self.started.is_set():
            self.started.set()
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
        if not self.finished.is_set():
            self.finished.wait(timeout)
        if not self.finished.is_set():
            raise TimeoutError()
        else:
            return self.Result

    def then(self, resolved=None, failed=None, print_exception=None):
        """
        The promise result. This function creates a new Promise which waits for current one, and calls corresponding function:
        :param resolved: Is called when function ended up conveniently. Receives result as a parameter.
        :param failed: Is called if function raises an exception, receives the exception.
        :param print_exception: Log level to output the exception, or None to mute it.
        :return: new Promise
        """
        self.print_exception = print_exception

        def wait_and_resolve():
            """
            Waits for current promise and resolves it.
            """
            self.wait()
            if self.resolved is True and callable(resolved):
                return resolved(self.Result)
            elif callable(failed):
                return failed(self.exception)

        p = Promise(wait_and_resolve)
        return p()

    def catch(self, callback=None, print_exception=None):
        """
        Same as Promise::then(None, callback)
        :param callback: Is called if function raises an exception, receives the exception.
        :param print_exception: Log level to output the exception, or None to mute it.
        :return: new Promise
        """
        return self.then(failed=callback, print_exception=print_exception)

    def run(self):
        """
        Thread entrance point.

        Runs the function and then the callback.
        """
        logging.debug("Thread: %s(%d), called from: %s(%d)" % (self.getName(), self.ident,
                                                               self.parent[0], self.parent[1]))
        try:
            self.Result = self.Callable(*self.args, **self.kwargs)
            self.resolved = True
        except Exception as e:
            info = sys.exc_info()
            if self.print_exception is not None:
                caught(e, info, level=self.print_exception)
            self.exception = e
            self.resolved = False
        finally:
            self.finished.set()


def async(function=None, daemon=False, print_exception=logging.ERROR):
    """
    Decorator around the functions for them to be asynchronous.

    Used as a plain decorator or along with named arguments.

    Usage:
    ```python
    @async
    def f1():
        pass

    @async(daemon=True)
    def f2():
        pass
    ```

    :param (Callable) function: The function to be decorated.
    :param (Boolean) daemon: Optional. Create the daemon thread, that will die when no other threads left.
    :param print_exception: Log level to log the exception, if any, or None to mute it.
    :return Callable: Wrapped function.
    """
    if function is None:
        def add_async_callback(func):
            """
            A second stage of a wrapper that is used if a wrapper is called with arguments.

            :param (Callable) func: The function to be decorated.
            :return Callable: Wrapped function.
            """
            return async(func, daemon, print_exception)
        return add_async_callback
    else:
        @wraps(function)
        def async_caller(*args, **kwargs):
            """
            An actual wrapper, that creates a thread.

            :return Promise: Thread of the function.
            """
            return Promise(function,
                           daemon=daemon,
                           print_exception=print_exception)(*args, **kwargs)
        return async_caller

if __name__ == '__main__':
    import time

    def test():
        print "waiting"
        time.sleep(5)
        print "end"
        return 5

    def test_fail():
        time.sleep(1)
        raise Exception("fail")

    def res(result):
        print "result = %s" % str(result)
        return "res"

    def err(exc):
        print "failed: %s" % str(exc)
        return "err"

    # print "test"
    # promise = Promise(test)()
    # promise.then(res, err)
    # promise.wait()
    #
    # time.sleep(1)
    # print "test_fail"
    # promise = Promise(test_fail)()
    # promise.then(res, err).then(res, err)
    # promise.wait()
    #
    # time.sleep(1)
    # print "resolve"
    # promise = Promise.resolve(1)
    # promise.then(res, err)
    #
    # time.sleep(1)
    # print "1"
    # promise = Promise(1)
    # promise.then(res, err)
    #
    # time.sleep(1)
    # print "reject"
    # promise = Promise.reject(1)
    # promise.then(res, err)
    #
    # time.sleep(1)
    # print "Promise test"
    # promise = Promise(Promise(test)())
    # promise.then(res, err)
    # promise.wait()
    #
    # time.sleep(1)
    # print "Promise test_fail"
    # promise = Promise(Promise(test_fail)())
    # promise.then(res, err).then(res, err)
    # promise.wait()
    #
    # time.sleep(1)
    # print "Promise all no reject"
    # promise = Promise.all([Promise(test)(), Promise(1)(), 2])
    # promise.then(res, err)
    # promise.wait()
    #
    # time.sleep(1)
    # print "Promise all reject"
    # promise = Promise.all([Promise(test)(), Promise(test_fail)(), Promise(1)(), 2])
    # promise.then(res, err)
    # promise.wait()
    #
    # time.sleep(1)
    # print "Promise race reject"
    # promise = Promise.race([Promise(test)(), Promise(test_fail)()])
    # promise.then(res, err)
    # promise.wait()
    #
    # time.sleep(1)
    # print "Promise race 1"
    # promise = Promise.race([Promise(test)(), Promise(test_fail)(), Promise(1)()])
    # promise.then(res, err)
    # promise.wait()
