"""
Welcome to Metaflow!

Metaflow is a microframework for data science projects.

There are two main use cases for this package:

1) You can define new flows using the `FlowSpec`
   class and related decorators.

2) You can access and inspect existing flows.
   You can instantiate a `Metaflow` class to
   get an entry point to all existing objects.

# How to work with flows

A flow is a directed graph of Python functions called steps.
Metaflow takes care of executing these steps one by one in various
environments, such as on a local laptop or compute environments
(such as AWS Batch for example). It snapshots
data and code related to each run, so you can resume, reproduce,
and inspect results easily at a later point in time.

Here is a high-level overview of objects related to flows:

 [ FlowSpec ]     (0) Base class for flows.
 [ MyFlow ]       (1) Subclass from FlowSpec to define a new flow.

define new flows
----------------- (2) Run MyFlow on the command line.
access results

 [ Flow ]         (3) Access your flow with `Flow('MyFlow')`.
 [ Run ]          (4) Access a specific run with `Run('MyFlow/RunID')`.
 [ Step ]         (5) Access a specific step by its name, e.g. `run['end']`.
 [ Task ]         (6) Access a task related to the step with `step.task`.
 [ DataArtifact ] (7) Access data of a task with `task.data`.

# More questions?

If you have any questions, feel free to post a bug report/question on the
Metaflow GitHub page.
"""

import importlib
import sys
import types

from os import path

CURRENT_DIRECTORY = path.dirname(path.abspath(__file__))
INFO_FILE = path.join(path.dirname(CURRENT_DIRECTORY), "INFO")

from metaflow.extension_support import (
    alias_submodules,
    get_modules,
    lazy_load_aliases,
    load_globals,
    load_module,
    EXT_PKG,
    _ext_debug,
)


# We load the module overrides *first* explicitly. Non overrides can be loaded
# in toplevel as well but these can be loaded first if needed. Note that those
# modules should be careful not to include anything in Metaflow at their top-level
# as it is likely to not work.
_override_modules = []
_tl_modules = []
try:
    _modules_to_import = get_modules("toplevel")

    for m in _modules_to_import:
        override_module = m.module.__dict__.get("module_overrides", None)
        if override_module is not None:
            _override_modules.append(
                ".".join([EXT_PKG, m.tl_package, "toplevel", override_module])
            )
        tl_module = m.module.__dict__.get("toplevel", None)
        if tl_module is not None:
            _tl_modules.append(".".join([EXT_PKG, m.tl_package, "toplevel", tl_module]))
    _ext_debug("Got overrides to load: %s" % _override_modules)
    _ext_debug("Got top-level imports: %s" % _tl_modules)
except Exception as e:
    _ext_debug("Error in importing toplevel/overrides: %s" % e)

# Load overrides now that we have them (in the proper order)
for m in _override_modules:
    extension_module = load_module(m)
    if extension_module:
        # We load only modules
        tl_package = m.split(".")[1]
        lazy_load_aliases(alias_submodules(extension_module, tl_package, None))

# Utilities
from .multicore_utils import parallel_imap_unordered, parallel_map
from .metaflow_profile import profile

# current runtime singleton
from .current import current

# Flow spec
from .flowspec import FlowSpec

from .parameters import Parameter, JSONTypeClass

JSONType = JSONTypeClass()

# data layer
from .datatools import S3

# includefile
from .includefile import IncludeFile

# Decorators
from .decorators import step, _import_plugin_decorators

# this auto-generates decorator functions from Decorator objects
# in the top-level metaflow namespace
_import_plugin_decorators(globals())
# Setting card import for only python 3.4
if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    from . import cards

# Client
from .client import (
    namespace,
    get_namespace,
    default_namespace,
    metadata,
    get_metadata,
    default_metadata,
    Metaflow,
    Flow,
    Run,
    Step,
    Task,
    DataArtifact,
)

__version_addl__ = []
_ext_debug("Loading top-level modules")
for m in _tl_modules:
    extension_module = load_module(m)
    if extension_module:
        tl_package = m.split(".")[1]
        load_globals(extension_module, globals(), extra_indent=True)
        lazy_load_aliases(
            alias_submodules(extension_module, tl_package, None, extra_indent=True)
        )
        version_info = getattr(extension_module, "__mf_extensions__", "<unk>")
        if extension_module.__version__:
            version_info = "%s(%s)" % (version_info, extension_module.__version__)
        __version_addl__.append(version_info)

if __version_addl__:
    __version_addl__ = ";".join(__version_addl__)
else:
    __version_addl__ = None

# Erase all temporary names to avoid leaking things
for _n in [
    "_ext_debug",
    "alias_submodules",
    "get_modules",
    "lazy_load_aliases",
    "load_globals",
    "load_module",
    EXT_PKG,
    "_override_modules",
    "_tl_modules",
    "_modules_to_import",
    "m",
    "override_module",
    "tl_module",
    "extension_module",
    "tl_package",
    "version_info",
]:
    try:
        del globals()[_n]
    except KeyError:
        pass
del globals()["_n"]

import pkg_resources

try:
    __version__ = pkg_resources.get_distribution("metaflow").version
except:
    # this happens on remote environments since the job package
    # does not have a version
    __version__ = None
