from __future__ import print_function
import inspect
import sys

from functools import partial

from .util import is_stringish

# Set
#
# - METAFLOW_DEBUG_SUBCOMMAND=1
#   to see command lines used to launch subcommands (especially 'step')
# - METAFLOW_DEBUG_SIDECAR=1
#   to see command lines used to launch sidecars
# - METAFLOW_DEBUG_S3CLIENT=1
#   to see command lines used by the S3 client. Note that this environment
#   variable also disables automatic cleaning of subdirectories, which can
#   fill up disk space quickly


class Debug(object):
    def __init__(self):
        import metaflow.metaflow_config as config

        have_debug_options = False
        for typ in config.DEBUG_OPTIONS:
            if getattr(config, "DEBUG_%s" % typ.upper()):
                op = partial(self.log, typ)
                have_debug_options = True
            else:
                op = self.noop
            # use debug.$type_exec(args) to log command line for subprocesses
            # of type $type
            setattr(self, "%s_exec" % typ, op)
            # use the debug.$type flag to check if logging is enabled for $type
            setattr(self, typ, op != self.noop)
        # In some environments (I'm looking at you Bazel), the path to the filename is
        # super long and not very useful. We will print it once and truncate the rest.
        # This is not 100% accurate as each package is in a separate directory so the
        # prefix length may be a bit different but it's good enough and removes a lot
        # of noise while also keeping the cost low (instead of having to figure out
        # the prefix for each package)
        self.prefix_len = 0
        if have_debug_options:
            # Figure out the name of the current file and strip out everything before
            #
            self.prefix_len = len(inspect.stack()[0][1][: -len("metaflow/debug.py")])
            self.log(
                "options",
                "File prefix is: %s" % inspect.stack()[0][1][: self.prefix_len],
            )

    def log(self, typ, args):
        if is_stringish(args):
            s = args
        else:
            s = " ".join(args)
        lineno = inspect.currentframe().f_back.f_lineno
        filename = inspect.stack()[1][1]
        print(
            "debug[%s %s:%s]: %s" % (typ, filename[self.prefix_len :], lineno, s),
            file=sys.stderr,
        )

    def __getattr__(self, name):
        # Small piece of code to get pyright and other linters to recognize that there
        # are dynamic attributes.
        return getattr(self, name)

    def noop(self, args):
        pass


debug = Debug()
