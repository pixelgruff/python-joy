# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 23:10:47 2016

@author: pixelgruff
"""

import time
import ctypes
import traceback

from functools import partial
from multiprocessing import Lock, Pool, Pipe, Value


# http://stackoverflow.com/questions/11892383/custom-python-traceback-or-debug-output
class RemoteException(Exception):
    """
    Messy, but necessary, class to store the stack trace inside an Exception
    so it can be communicated between processes
    """
    def __init__(self, trace):
        Exception.__init__(self, trace)


# Inspired by:
# http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing
# who explains that multiprocessing.Value is extremely misleading
class CoarseLockedValue(object):
    def __init__(self, ctype, initval):
        self.val = Value(ctype, initval)
        self.lock = Lock()

    def set_value(self, value):
        with self.lock:
            self.val.value = value

    def get_value(self):
        with self.lock:
            return self.val.value


class CoarseLockedInt(CoarseLockedValue):
    def __init__(self, initval=0):
        super(CoarseLockedInt, self).__init__(ctypes.c_int, initval)

    def increment(self):
        with self.lock:
            self.val.value += 1

    def decrement(self):
        with self.lock:
            self.val.value -= 1


class CoarseLockedBool(CoarseLockedValue):
    def __init__(self, initval=False):
        super(CoarseLockedBool, self).__init__(ctypes.c_bool, initval)


# Ensure that functions will return control to the caller even if they raise an exception
def return_safely(func, element, args, kwargs):
    try:
        return func(element, *args, **kwargs)
    except Exception:
        trace = traceback.format_exc()
        return RemoteException(trace)


# Handle results and exceptions, while communicating the state of the Pool over CoarseLocked objects
def results_to_pipe(result, counter, flag, connection):
    # Check for an exception and throw the failure flag
    if isinstance(result, RemoteException):
        flag.set_value(True)
        # If we encountered an exception, send it across the pipe
        connection.send(result)
    else:
        # Discard the result (currently) and decrement the count of running processes
        counter.decrement()


class SafePool:
    """
    Vanilla Python class for applying a function asynchronously.

    SafePool manages inter-process communication via callback functions
    and inter-thread communication via a shared Pipe.
    """

    def __init__(self, processes=4):
        self.process_limit = processes

    def apply_async(self, f, generator, *args, **kwargs):
        """
        Asynchronously apply the given function across a generator
        """

        pool = Pool(processes=self.process_limit)
        rx, tx = Pipe(duplex=False)

        # Limit the number of queued tasks to the number of processes
        process_counter = CoarseLockedInt(0)
        failure_flag = CoarseLockedBool(False)

        safe_func = partial(return_safely, func=f, args=args, kwargs=kwargs)
        callback_func = partial(results_to_pipe, counter=process_counter, flag=failure_flag, connection=tx)

        # Loop over generator or iterable, allocating new processes as consumers, up to the allocation limit
        iter_gen = iter(generator())
        while True:
            if process_counter.get_value() < self.process_limit:
                try:
                    element = iter_gen.next()
                    pool.apply_async(safe_func,
                                     kwds={"element": element},
                                     callback=callback_func)

                    process_counter.increment()
                    # print("Processes running: {0}".format(process_counter.get_value()))

                except StopIteration:
                    break
            else:
                # Sleep while waiting for processes to complete and become available
                time.sleep(1)
            if failure_flag.get_value():  # Exception thrown
                raise rx.recv()

        # Ensure remaining processes terminate successfully by polling the process count and failure flag
        while process_counter.get_value() > 0:
            if failure_flag.get_value():
                raise rx.recv()
            time.sleep(1)

        pool.close()
        pool.join()


def test_generator():
    for i in range(20):
        yield i


def prejudiced_consumer(i, prejudice):
    time.sleep(1)
    if i == prejudice:
        raise ValueError("I don't like the number {0}!".format(prejudice))
    else:
        with open("whatever-{0}.txt".format(i), 'w') as outfile:
            outfile.write("{0}".format(i ** 2))


if __name__ == "__main__":
    print("Executing!")

    pool = SafePool(processes=3)

    pool.apply_async(prejudiced_consumer, test_generator, prejudice=10)
