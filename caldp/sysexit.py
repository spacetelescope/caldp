"""This module defines context managers related to exception hanlding and retries.

The exit_on_exception() context manager is used to trap exceptions and exit
Python cleanly with a numerical exit code which is ultimately reported by the
Batch job:

  with exit_on_exception(exit_codes.CODE, "details of this trap"):
      ... python statements to guard ...

exit_on_exception() will nominally map any failure occurring in that context
onto exit_codes.CODE and perform these functions:

  1. Descriptive text about the code block ("details of this trap") is logged.

  2. An abbreviated traceback is output.

  3. sys.exit() is called with a well defined CALDP exit code and the program terminates.

All of the CALDP exit codes are defined in caldp.exit_codes with the intent
that no other codes be reported.  Subprocess exit codes and Python exit codes
are mapped onto codes from caldp.exit_codes disambiguating the status reported
to AWS Batch.  While more detaied information is logged, these numerical codes
are accessible to the Batch job error handler.

Three kinds of exceptions are handled specially:

  1. Various forms of memory errors are identified and mapped onto exit codes
     which supersede exit_codes.CODE.   These typically drive job rescues.

  2. A CALDP SubprocessFailure may result in advisory output which attempts to
     identify the UNIX signal which terminated the subprocess.

  3. The string MemoryError appearing in the program log is detected by the
     caldp-process script and augments this error handling with an additional
     class of memory errors which can be rescued.  Presumably this originates
     in a CALDP subprocess which traps the exception but nevertheless fails
     anyway.   In this case caldp-process replaces the exit code reported by
     caldp programs with one which identifies the memory error.

If no exception or memory error occurs in the specified context,  exit_on_exception()
does nothing.

---

For doing doctests,  a little global setup is needed:

>>> from caldp import log
>>> log.set_test_mode()
>>> log.reset()

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


class SubprocessFailure(Exception):
    """A called subprocess failed and may require signal reporting.

    In Python, a negative subprocess returncode indicates that the absolute
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
def test_sys_exit():
    """Support testing sys.exit() calls by trapping them and displaying exit code
    instead of actually exiting.
    """
    try:
        yield
    except SystemExit as exc:
        print(f"sys.exit({exc.code})")


@contextlib.contextmanager
def exit_on_exception(exit_code=exit_codes.GENERIC_ERROR, *args):
    """exit_on_exception is a context manager which issues an error message
    based on *args and then calls sys.exit(exit_code) whenever an exception
    is raised within the corresponding "with block".

    >>> with exit_on_exception(1, "As expected", "it did not fail."):
    ...    print("do it.")
    do it.

    >>> with test_sys_exit():  #doctest: +ELLIPSIS
    ...    with exit_on_exception(2, "As expected", "it failed."):
    ...        raise Exception("It failed!")
    ...        print("do it.")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - As expected it failed.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../sysexit.py", line ..., in exit_on_exception
    ERROR -     yield
    ERROR -   File "<doctest ...exit_on_exception[1]>", line ..., in <module>
    ERROR -     raise Exception("It failed!")
    ERROR - Exception: It failed!
    EXIT - CMDLINE_ERROR[2]: The program command line invocation was incorrect.
    sys.exit(2)

    Never printed 'do it.'  SystemExit is caught for testing and the exit code is displayed.

    >>> with test_sys_exit():  #doctest: +ELLIPSIS
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
    sys.exit(23)

    An OSError which includes "Cannot allocate memory" in its str() or repr() is trapped as
    an appropriate CALDP memory error:

    >>> with test_sys_exit():  #doctest: +ELLIPSIS
    ...     with exit_on_exception(exit_codes.STAGE1_ERROR, "Failure running processing stage1."):
    ...         raise OSError("Cannot allocate memory")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Failure running processing stage1.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../caldp/sysexit.py", line ..., in exit_on_exception
    ERROR -     yield
    ERROR -   File "<doctest caldp.sysexit.exit_on_exception[...]>", line ..., in <module>
    ERROR -     raise OSError("Cannot allocate memory")
    ERROR - OSError: Cannot allocate memory
    EXIT - OS_MEMORY_ERROR[34]: Python raised OSError(Cannot allocate memory...),  possibly fork failure.
    sys.exit(34)

    An OSError w/o "Cannot allocate memory" in its str() is treated normally:

    >>> with test_sys_exit():  #doctest: +ELLIPSIS
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
    sys.exit(23)

    Nested traps report from the innermost handler:

    >>> with test_sys_exit():  #doctest: +ELLIPSIS
    ...     with exit_on_exception(exit_codes.STAGE1_ERROR, "Failure running processing stage1."):
    ...         with exit_on_exception(exit_codes.STAGE2_ERROR, "Failure running processing stage2."):
    ...             raise OSError("Something other than memory")
    ERROR - ----------------------------- Fatal Exception -----------------------------
    ERROR - Failure running processing stage2.
    ERROR - Traceback (most recent call last):
    ERROR -   File ".../caldp/sysexit.py", line ..., in exit_on_exception
    ERROR -     yield
    ERROR -   File "<doctest caldp.sysexit.exit_on_exception[...]>", line ..., in <module>
    ERROR -     raise OSError("Something other than memory")
    ERROR - OSError: Something other than memory
    EXIT - STAGE2_ERROR[24]: An error occurred in this instrument's stage2 processing step, e.g astrodrizzle
    sys.exit(24)
    """
    try:
        yield
    except SystemExit:
        raise
    except MemoryError:
        _report_exception(exit_codes.CALDP_MEMORY_ERROR, args)
    except OSError as exc:
        if "Cannot allocate memory" in str(exc) + repr(exc):
            _report_exception(exit_codes.OS_MEMORY_ERROR, args)
        else:
            _report_exception(exit_code, args)
    except SubprocessFailure as exc:
        _report_exception(exit_code, args, exc.returncode)
    except Exception:
        _report_exception(exit_code, args)


def _report_exception(exit_code, args=None, returncode=None):
    """Issue trigger output for exit_on_exception, including `exit_code` and
    error message defined by `args`, as well as traceback.

    If `return_code` is specified and negative then assume it describes the
    UNIX signal which terminated a subprocess and provide a text
    interpretation in the log output.

    Exits - This function calls sys.exit() with `exit_code` and never returns.
            The resulting SysExit exception is not trapped by CALDP and
            silently unwinds the program stack.  It is eventually caught by
            Python to do program cleanup such as flushing output buffers before
            exiting with `exit_code` as the numerical program status.
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
    sys.exit(exit_code)


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
    return max(min(random.uniform(0.5, 1) * backoff ** iteration, max_sleep), min_sleep)


# ==============================================================================


def test():  # pragma: no cover
    from doctest import testmod
    import caldp.sysexit

    test_result = testmod(caldp.sysexit)
    return test_result


if __name__ == "__main__":  # pragma: no cover
    print(test())
