"""
metaflow._vendor is for vendoring dependencies of metaflow to prevent needing metaflow to
depend on something external.
Files inside of metaflow._vendor should be considered immutable and should only be
updated to versions from upstream.
"""
from __future__ import absolute_import

import glob
import os.path
import sys


# Define a small helper function to alias our vendored modules to the real ones
# if the vendored ones do not exist. This idea of this was taken from
# https://github.com/kennethreitz/requests/pull/2567.
def vendored(modulename):
    vendored_name = "{0}.{1}".format(__name__, modulename)

    try:
        __import__(modulename, globals(), locals(), level=0)
    except ImportError:
        # We can just silently allow import failures to pass here. If we
        # got to this point it means that ``import pip._vendor.whatever``
        # failed and so did ``import whatever``. Since we're importing this
        # upfront in an attempt to alias imports, not erroring here will
        # just mean we get a regular import error whenever pip *actually*
        # tries to import one of these modules to use it, which actually
        # gives us a better error message than we would have otherwise
        # gotten.
        pass
    else:
        sys.modules[vendored_name] = sys.modules[modulename]
        base, head = vendored_name.rsplit(".", 1)
        setattr(sys.modules[base], head, sys.modules[modulename])

