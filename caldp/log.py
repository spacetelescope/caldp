"""Some simple console message functions w/counting features for
errors, warnings, and info.

>>> from caldp import log

>>> log.warning("this is a test warning.")
WARNING - this is a test warning.

>>> log.error("this is a test error.")
ERROR - this is a test error.

>>> log.info("this is just informative.")
INFO - this is just informative.

>>> log.debug("Normally these generate no output")

>>> log.echo("Emit str verbatim to log subprocesses")
Emit str verbatim to log subprocesses

"""

# ==============================================================================


class SimpleLogger:
    def __init__(self, level):
        self.level = level

    def __call__(self, *args):
        if self.level != "DEBUG":
            level = self.level + " - " if self.level else ""
            msg = level + " ".join([str(arg) for arg in args])
            print(msg)
            if HANDLE:
                print(msg, file=HANDLE)


def init_log(filename=None, mode="w+"):
    global info, warning, error, debug, echo, HANDLE
    HANDLE = open(filename, mode) if filename else None
    info = SimpleLogger("INFO")
    warning = SimpleLogger("WARNING")
    error = SimpleLogger("ERROR")
    debug = SimpleLogger("DEBUG")
    echo = SimpleLogger("")


def close_log():
    global HANDLE
    HANDLE.close()
    HANDLE = None


init_log()

# ==============================================================================


def divider(name="", char="-", n=75, func=info, **keys):
    """Create a log divider line consisting of `char` repeated `n` times
    possibly with `name` injected into the center of the divider.
    Output it as a string to logging function `func` defaulting to info().
    """
    if name:
        n2 = (n - len(name) - 2) // 2
        func(char * n2, name, char * n2, **keys)
    else:
        func(char * n, **keys)


# ===================================================================


def test():
    from caldp import log
    import doctest

    return doctest.testmod(log)


if __name__ == "__main__":
    print(test())
