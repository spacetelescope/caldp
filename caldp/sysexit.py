"""This module defines context managers which are used to trap exceptions
and exit Python cleanly with specific exit_codes which are then seen as
the numerical exit status of the process and ultimately Batch job.

The exit_on_exception() context manager is used to bracket a block of code
by mapping all exceptions onto some log output and a call to sys.exit():

    with exit_on_exception(exit_codes.SOME_CODE, "Parts of the ERROR", "message output", "on exception."):
        ... the code you're trapping to SOME_CODE when things go wrong ...

The exit_on_exception() manager also enables simulating errors by defining the
CALDP_SIMULATE_ERROR=N environment variable.  When the manager is called with a
code matching CALDP_SIMULATE_ERROR, instead of running the code block it fakes
an exception by performing the corresponding log output and sys.exit() call.  A
few error codes are simulated more directly, particularly memory errors.

The exit_receiver() manager is used to bracket the top level of your code,
nominally main(), and land the CaldpExit() exception raised by
exit_on_exception() after the stack has been unwound and cleanup functions
performed.  exit_receiver() then exits Python with the error code originally
passed into exit_on_exception().
"""

import sys
import os
import contextlib
import traceback
import resource
import time
import random

from caldp import log
from caldp import exit_codes

# ==============================================================================


class CaldpExit(SystemExit):
    """Handle like SystemExit,  but we definitely threw it."""


class SubprocessFailure(Exception):
    """A called subprocess failed and may require signal reporting.

    In Python, a negative subprocess returncode indicates that the absolete
    value of the returncode is a signal number which killed the subprocess.

    For completeness, in Linux, the program exit_code is a byte value.  If the
    sign bit is set, a signal and/or core dump occurred.  The byte reported as
    exit_code may be unsigned.  The lower bits of the returncode define either
    the program's exit status or a signum identifying the signal which killed
    the process.

    """

    def __init__(self, returncode):
        self.returncode = returncode


@contextlib.contextmanager
def exit_on_exception(exit_code, *args):
    """exit_on_exception is a context manager which issues an error message
    based on *args and then does sys.exit(exit_code) if an exception is
    raised within the corresponding "with block".

    >>> with exit_on_exception(1, "As expected", "it did not fail."):
    ...    print("do it.")
    do it.

    >>> try: #doctest: +ELLIPSIS
    ...    with exit_on_exception(2, "As expected", "it failed."):
    ...        raise Exception("It failed!")
    ...        print("do it.")
    ... except SystemExit:
    ...    log.divider()
    ...    print("Trapping SystemExit normally caught by exit_reciever() at top level.")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - As expected it failed.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_on_exception
    ERROR -     yield
    ERROR -   File "<doctest ...exit_on_exception[1]>", line ..., in <module>
    ERROR -     raise Exception("It failed!")
    ERROR - Exception: It failed!
    EXIT - CMDLINE_ERROR[2]: The program command line invocation was incorrect.
    INFO - ---------------------------------------------------------------------------
    Trapping SystemExit normally caught by exit_reciever() at top level.

    Never printed 'do it.'  SystemExit is caught for testing.

    If CALDP_SIMULATE_ERROR is set to one of exit_codes, it will cause the
    with exit_on_exception() block to act as if a failure has occurred:

    >>> os.environ["CALDP_SIMULATE_ERROR"] = "2"
    >>> try: #doctest: +ELLIPSIS
    ...    with exit_on_exception(2, "As expected a failure was simulated"):
    ...        print("should not see this")
    ... except SystemExit:
    ...    pass
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - As expected a failure was simulated
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_on_exception
    ERROR -     raise RuntimeError(f"Simulating error = {simulated_code}")
    ERROR - RuntimeError: Simulating error = 2
    EXIT - CMDLINE_ERROR[2]: The program command line invocation was incorrect.

    >>> os.environ["CALDP_SIMULATE_ERROR"] = str(exit_codes.CALDP_MEMORY_ERROR)
    >>> try: #doctest: +ELLIPSIS
    ...    with exit_on_exception(2, "Memory errors don't have to match"):
    ...        print("Oh unhappy day.")
    ... except SystemExit:
    ...    pass
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Memory errors don't have to match
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_on_exception
    ERROR -     raise MemoryError("Simulated CALDP MemoryError.")
    ERROR - MemoryError: Simulated CALDP MemoryError.
    EXIT - CALDP_MEMORY_ERROR[32]: CALDP generated a Python MemoryError during processing or preview creation.

    >>> os.environ["CALDP_SIMULATE_ERROR"] = str(exit_codes.OS_MEMORY_ERROR)
    >>> try: #doctest: +ELLIPSIS
    ...    with exit_on_exception(2, "Memory errors don't have to match"):
    ...        print("Oh unhappy day.")
    ... except SystemExit:
    ...    pass
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Memory errors don't have to match
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_on_exception
    ERROR -     raise OSError("Cannot allocate memory...")
    ERROR - OSError: Cannot allocate memory...
    EXIT - OS_MEMORY_ERROR[34]: Python raised OSError(Cannot allocate memory...),  possibly fork failure.

    >>> os.environ["CALDP_SIMULATE_ERROR"] = "999"
    >>> with exit_on_exception(3, "Only matching error codes are simulated."):
    ...    print("should print normally")
    should print normally

    >>> del os.environ["CALDP_SIMULATE_ERROR"]

    >>> saved, os._exit = os._exit, lambda x: print(f"os._exit({x})")
    >>> with exit_receiver():  #doctest: +ELLIPSIS
    ...     with exit_on_exception(exit_codes.STAGE1_ERROR, "Failure running processing stage1."):
    ...         raise SubprocessFailure(-8)
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Failure running processing stage1.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../caldp/sysexit.py", line ..., in exit_on_exception
    ERROR -     yield
    ERROR -   File "<doctest caldp.sysexit.exit_on_exception[...]>", line ..., in <module>
    ERROR -     raise SubprocessFailure(-8)
    ERROR - caldp.sysexit.SubprocessFailure: -8
    EXIT - Killed by UNIX signal SIGFPE[8]: 'Floating-point exception (ANSI).'
    EXIT - STAGE1_ERROR[23]: An error occurred in this instrument's stage1 processing step. e.g. calxxx
    os._exit(23)

    >>> with exit_receiver():  #doctest: +ELLIPSIS
    ...     with exit_on_exception(exit_codes.STAGE1_ERROR, "Failure running processing stage1."):
    ...         raise OSError("Something other than memory")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Failure running processing stage1.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_on_exception
    ERROR -     yield
    ERROR -   File "<doctest ...sysexit.exit_on_exception[...]>", line ..., in <module>
    ERROR -     raise OSError("Something other than memory")
    ERROR - OSError: Something other than memory
    EXIT - STAGE1_ERROR[23]: An error occurred in this instrument's stage1 processing step. e.g. calxxx
    os._exit(23)

    >>> os._exit = saved
    """
    simulated_code = int(os.environ.get("CALDP_SIMULATE_ERROR", "0"))
    try:
        if simulated_code == exit_codes.CALDP_MEMORY_ERROR:
            raise MemoryError("Simulated CALDP MemoryError.")
        elif simulated_code == exit_codes.OS_MEMORY_ERROR:
            raise OSError("Cannot allocate memory...")
        elif simulated_code == exit_codes.SUBPROCESS_MEMORY_ERROR:
            print("MemoryError", file=sys.stderr)  # Output to process log determines final program exit status
            raise RuntimeError("Simulated subprocess memory error with subsequent generic program exception.")
        elif simulated_code == exit_codes.CONTAINER_MEMORY_ERROR:
            log.info("Simulating hard memory error by allocating memory")
            _ = bytearray(1024 * 2**30)  # XXXX does not trigger container limit as intended
        elif exit_code == simulated_code:
            raise RuntimeError(f"Simulating error = {simulated_code}")
        yield
    # don't mask memory errors or nested exit_on_exception handlers
    except MemoryError:
        _report_exception(exit_codes.CALDP_MEMORY_ERROR, args)
        raise CaldpExit(exit_codes.CALDP_MEMORY_ERROR)
    except OSError as exc:
        if "Cannot allocate memory" in str(exc) + repr(exc):
            _report_exception(exit_codes.OS_MEMORY_ERROR, args)
            raise CaldpExit(exit_codes.OS_MEMORY_ERROR)
        else:
            _report_exception(exit_code, args)
            raise CaldpExit(exit_code)
    except CaldpExit:
        raise
    # below as always exit_code defines what will be CALDP's program exit status.
    # in contrast,  exc.returncode is the subprocess exit status of a failed subprocess which may
    # define an OS signal that killed the process.
    except SubprocessFailure as exc:
        _report_exception(exit_code, args, exc.returncode)
        raise CaldpExit(exit_code)
    except Exception:
        _report_exception(exit_code, args)
        raise CaldpExit(exit_code)


def _report_exception(exit_code, args=None, returncode=None):
    """Issue trigger output for exit_on_exception, including `exit_code` and
    error message defined by `args`, as well as traceback.
    """
    log.divider("Fatal Exception", func=log.error)
    if args:
        log.error(*args)
    for line in traceback.format_exc().splitlines():
        if line != "NoneType: None":
            log.error(line)
    if returncode and returncode < 0:
        print(exit_codes.explain_signal(-returncode))
    print(exit_codes.explain(exit_code))


@contextlib.contextmanager
def exit_receiver():
    """Use this contextmanager to bracket your top level code and land the sys.exit()
    exceptions thrown by _raise_exit_exception() and exit_on_exception().

    This program structure enables sys.exit() to fully unwind the stack doing
    cleanup, then calls the low level os._exit() function which does no cleanup
    as the "last thing".

    If SystemExit is not raised by the code nested in the "with" block then
    exit_receiver() essentially does nothing.

    The program is exited with the numerical code passed to sys.exit().

    >>> saved, os._exit = os._exit, lambda x: print(f"os._exit({x})")

    >>> with exit_receiver():  #doctest: +ELLIPSIS
    ...     print("Oh happy day.")
    Oh happy day.
    os._exit(0)

    Generic unhandled exceptions are mapped to GENERIC_ERROR (1):

    >>> def foo():
    ...    print("foo!")
    ...    bar()
    >>> def bar():
    ...    print("bar!")
    ...    raise RuntimeError()

    >>> with exit_receiver(): #doctest: +ELLIPSIS
    ...     foo()
    foo!
    bar!
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Untrapped non-memory exception.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../caldp/sysexit.py", line ..., in exit_receiver
    ERROR -     yield  # go off and execute the block
    ERROR -   File "<doctest caldp.sysexit.exit_receiver[...]>", line ..., in <module>
    ERROR -     foo()
    ERROR -   File "<doctest caldp.sysexit.exit_receiver[...]>", line ..., in foo
    ERROR -     bar()
    ERROR -   File "<doctest caldp.sysexit.exit_receiver[...]>", line ..., in bar
    ERROR -     raise RuntimeError()
    ERROR - RuntimeError
    EXIT - GENERIC_ERROR[1]: An error with no specific CALDP handling occurred somewhere.
    os._exit(1)

    MemoryError is remapped to CALDP_MEMORY_ERROR (32) inside exit_on_exception or not:

    >>> with exit_receiver(): #doctest: +ELLIPSIS
    ...     raise MemoryError("CALDP used up all memory directly.")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Untrapped memory exception.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../caldp/sysexit.py", line ..., in exit_receiver
    ERROR -     yield  # go off and execute the block
    ERROR -   File "<doctest caldp.sysexit.exit_receiver[...]>", line ..., in <module>
    ERROR -     raise MemoryError("CALDP used up all memory directly.")
    ERROR - MemoryError: CALDP used up all memory directly.
    EXIT - CALDP_MEMORY_ERROR[32]: CALDP generated a Python MemoryError during processing or preview creation.
    os._exit(32)

    Inside exit_on_exception, exit status is remapped to the exit_code parameter
    of exit_on_exception():

    >>> with exit_receiver(): #doctest: +ELLIPSIS
    ...     raise OSError("Cannot allocate memory...")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Untrapped OSError cannot callocate memory
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_receiver
    ERROR -     yield  # go off and execute the block
    ERROR -   File "<doctest ...sysexit.exit_receiver[...]>", line ..., in <module>
    ERROR -     raise OSError("Cannot allocate memory...")
    ERROR - OSError: Cannot allocate memory...
    EXIT - OS_MEMORY_ERROR[34]: Python raised OSError(Cannot allocate memory...),  possibly fork failure.
    os._exit(34)

    >>> with exit_receiver(): #doctest: +ELLIPSIS
    ...     raise OSError("Some non-memory os error.")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Untrapped OSError, generic.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_receiver
    ERROR -     yield  # go off and execute the block
    ERROR -   File "<doctest ...sysexit.exit_receiver[...]>", line ..., in <module>
    ERROR -     raise OSError("Some non-memory os error.")
    ERROR - OSError: Some non-memory os error.
    EXIT - GENERIC_ERROR[1]: An error with no specific CALDP handling occurred somewhere.
    os._exit(1)

    >>> with exit_receiver(): #doctest: +ELLIPSIS
    ...    with exit_on_exception(exit_codes.STAGE1_ERROR, "Stage1 processing failed for <ippssoot>"):
    ...        raise RuntimeError("Some obscure error")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Stage1 processing failed for <ippssoot>
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_on_exception
    ERROR -     yield
    ERROR -   File "<doctest ...sysexit.exit_receiver[...]>", line ..., in <module>
    ERROR -     raise RuntimeError("Some obscure error")
    ERROR - RuntimeError: Some obscure error
    EXIT - STAGE1_ERROR[23]: An error occurred in this instrument's stage1 processing step. e.g. calxxx
    os._exit(23)

    >>> os._exit = saved

    """
    try:
        # log.info("Container memory limit is: ", get_linux_memory_limit())
        yield  # go off and execute the block
        code = exit_codes.SUCCESS
    except CaldpExit as exc:
        code = exc.code
        # Already reported deeper
    except MemoryError:
        code = exit_codes.CALDP_MEMORY_ERROR
        _report_exception(code, ("Untrapped memory exception.",))
    except OSError as exc:
        if "Cannot allocate memory" in str(exc) + repr(exc):
            code = exit_codes.OS_MEMORY_ERROR
            args = ("Untrapped OSError cannot callocate memory",)
        else:
            code = exit_codes.GENERIC_ERROR
            args = ("Untrapped OSError, generic.",)
        _report_exception(code, args)
    except BaseException:  # Catch absolutely everything.
        code = exit_codes.GENERIC_ERROR
        _report_exception(code, ("Untrapped non-memory exception.",))
    os._exit(code)


def get_linux_memory_limit():  # pragma: no cover
    """This generally shows the full address space by default.

    >> limit = get_linux_memory_limit()
    >> assert isinstance(limit, int)
    """
    if os.path.isfile("/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as limit:
            mem = int(limit.read())
        return mem
    else:
        raise RuntimeError("get_linux_memory_limit() failed.")  # pragma: no cover


def set_process_memory_limit(mem_in_bytes):
    """This can be used to limit the available address space / memory to
    something less than is allocated to the container.   Potentially that
    will cause Python to generate a MemoryError rather than forcing a
    container memory limit kill.
    """
    resource.setrlimit(resource.RLIMIT_AS, (mem_in_bytes, mem_in_bytes))  # pragma: no cover


# ==============================================================================


def retry(func, max_retries=3, min_sleep=1, max_sleep=60, backoff=2, exceptions=(Exception, SystemExit)):
    """a decorator for retrying a function call on exception
    max_retries: number of times to retry
    min_sleep: starting value for backing off, in seconds
    max_sleep: sleep value not to exceed, in seconds
    backoff: the exponential factor
    exceptions: tuple of exceptions to catch and retry

    """

    def decor(*args, **kwargs):
        tried = 0
        while tried < max_retries:
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                # otherwise e is lost to the namespace cleanup,
                # and we may need to raise it later
                exc = e
                tried += 1
                sleep = exponential_backoff(tried)
                log.warning(
                    f"{func.__name__} raised exception, using retry {tried} of {max_retries}, sleeping for {sleep} seconds "
                )
                time.sleep(sleep)

        # if we're here, no attempt to call func() succeeded
        raise exc

    return decor


def exponential_backoff(iteration, min_sleep=1, max_sleep=64, backoff=2):
    """given the current number of attempts, return a sleep time using an exponential backoff algorithm
    iteration: the current amount of retries used
    min_sleep: minimum value to wait before retry, in seconds
    max_sleep: maximum value to wait before retry, in seconds

    note: if you allow too many retries that cause the backoff to exceed max_sleep,
    you will lose the benefit of jitter
    see i.e. https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """

    # random uniform number(0.5,1) * backoff^iteration, but clip to min_backoff, max_backoff
    return max(min(random.uniform(0.5, 1) * backoff**iteration, max_sleep), min_sleep)


# ==============================================================================


def test():  # pragma: no cover
    from doctest import testmod
    import caldp.sysexit

    temp, os._exit = os._exit, lambda x: print(f"os._exit({x})")
    test_result = testmod(caldp.sysexit)
    os._exit = temp
    return test_result


if __name__ == "__main__":  # pragma: no cover
    print(test())
