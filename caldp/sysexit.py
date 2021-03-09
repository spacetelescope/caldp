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
nominally main(), and land the SystemExit() exception raised by
exit_on_exception() after the stack has been unwound and cleanup functions
performed.  exit_receiver() then exits Python with the error code originally
passed into exit_on_exception().

>>> from caldp import log
>>> log.set_test_mode()
>>> log.reset()
"""
import sys
import os
import contextlib
import traceback
import resource

from caldp import log
from caldp import exit_codes

# ==============================================================================


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
    ...    print("Trapping SystemExit normally caught by log.exit_reciever() at top level.")
    INFO - ----------------------------- Fatal Exception -----------------------------
    ERROR - EXIT CMDLINE_ERROR[2]: The program command line invocation was incorrect.
    ERROR - As expected it failed.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../caldp/caldp/sysexit.py", line ..., in exit_on_exception
    ERROR -     yield
    ERROR -   File "<doctest caldp.sysexit.exit_on_exception[1]>", line ..., in <module>
    ERROR -     raise Exception("It failed!")
    ERROR - Exception: It failed!
    INFO - ---------------------------------------------------------------------------
    Trapping SystemExit normally caught by log.exit_reciever() at top level.

    Never printed 'do it.'  SystemExit is caught for testing.

    If CALDP_SIMULATE_ERROR is set to one of exit_codes, it will cause the
    with exit_on_exception() block to act as if a failure has occurred:

    >>> os.environ["CALDP_SIMULATE_ERROR"] = "2"
    >>> try: #doctest: +ELLIPSIS
    ...    with exit_on_exception(2, "As expected a matching failure was simulated"):
    ...        print("should not see this")
    ... except SystemExit:
    ...    pass
    INFO - ----------------------------- Fatal Exception -----------------------------
    ERROR - EXIT CMDLINE_ERROR[2]: The program command line invocation was incorrect.
    ERROR - As expected a matching failure was simulated
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../caldp/caldp/sysexit.py", line ..., in exit_on_exception
    ERROR -     raise RuntimeError(f"Simulating error = {simulated_code}")
    ERROR - RuntimeError: Simulating error = 2

    >>> with exit_on_exception(3, "Only matching error codes are simulated."):
    ...    print("should print normally")
    should print normally

    >>> del os.environ["CALDP_SIMULATE_ERROR"]
    """
    simulated_code = int(os.environ.get("CALDP_SIMULATE_ERROR", "0"))
    try:
        if simulated_code == exit_codes.CALDP_MEMORY_ERROR:
            raise MemoryError("Simulated CALDP memory error.")
        elif simulated_code == exit_codes.SUBPROCESS_MEMORY_ERROR:
            print("MemoryError", file=sys.stderr)
            raise MemoryError("Simulated subprocess memory error.")
        elif simulated_code == exit_codes.CONTAINER_MEMORY_ERROR:
            log.info("Simulating hard memory error by allocating memory")
            _ = bytearray(1024 * 2 ** 30)  # 1024G XXXX NOT WORKING
        elif exit_code == simulated_code:
            raise RuntimeError(f"Simulating error = {simulated_code}")
        yield
    # don't mask memory errors or nested exit_on_exception handlers
    except SystemExit as exc:
        _report_exception(exc.code)
        raise
    except MemoryError:
        _report_exception(exit_codes.CALDP_MEMORY_ERROR)
        raise
    except Exception:
        _raise_exit_exception(exit_code, *args)


def _raise_exit_exception(exit_code, *args):
    """Call this function to terminate program execution with the error message
    defined by `*args` eventually exiting with `exit_code`.

    Each element of positional parameters *args is first converted to a string,
    then all elements are joined seperated by spaces to form the completed'
    exception ERROR message.

    >>> try:
    ...    _raise_exit_exception(exit_codes.GENERIC_ERROR, "this is a test exit exception.")
    ...    print("won't get here.")
    ... except SystemExit:
    ...    log.divider()
    ...    print("Trapping SystemExit normally caught by log.exit_reciever() at top level.")
    INFO - ----------------------------- Fatal Exception -----------------------------
    ERROR - EXIT GENERIC_ERROR[1]: An error with no specific CALDP handling occurred somewhere.
    ERROR - this is a test exit exception.
    INFO - ---------------------------------------------------------------------------
    Trapping SystemExit normally caught by log.exit_reciever() at top level.
    """
    _report_exception(exit_code, *args)
    sys.exit(exit_code)


def _report_exception(exit_code, *args):
    log.divider("Fatal Exception")
    log.error(exit_codes.explain(exit_code))
    if args:
        log.error(*args)
    for line in traceback.format_exc().splitlines():
        if line != "NoneType: None":
            log.error(line)


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
    """
    try:
        log.info("Container memory limit is: ", get_linux_memory_limit())
        yield
    except SystemExit as exc:
        os._exit(exc.code)
    except MemoryError:
        os._exit(exit_codes.CALDP_MEMORY_ERROR)
    except Exception:
        os._exit(exit_codes.GENERIC_ERROR)
    os._exit(exit_codes.SUCCESS)


def get_linux_memory_limit():
    """This generally shows the full address space by default."""
    if os.path.isfile("/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as limit:
            mem = int(limit.read())
        return mem
    else:
        raise RuntimeError("get_linux_memory_limit() failed.")


def set_process_memory_limit(mem_in_bytes):
    """This can be used to limit the available address space / memory to
    something less than is allocated to the container.   Potentially that
    will cause Python to generate a MemoryError rather than forcing a
    container memory limit kill.
    """
    resource.setrlimit(resource.RLIMIT_AS, (mem_in_bytes, mem_in_bytes))


def test():
    from caldp import sysexit
    import doctest

    return doctest.testmod(sysexit)


if __name__ == "__main__":
    print(test())
