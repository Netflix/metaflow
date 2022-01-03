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
Metaflow Github page.
"""

import importlib
import sys
import types

if sys.version_info[0] >= 3 and sys.version_info[1] >= 4:
    import importlib.util
    from importlib.machinery import ModuleSpec
else:
    # Something random so there is no syntax error
    ModuleSpec = None


class _LazyLoader(object):
    # This _LazyLoader implements the Importer Protocol defined in PEP 302
    # TODO: Need to move to find_spec, exec_module and create_module as
    # find_module and load_module are deprecated

    def __init__(self, handled):
        # Modules directly loaded (this is either new modules or overrides of existing ones)
        self._handled = handled if handled else {}

        # This is used to revert back to regular loading when trying to load
        # the over-ridden module
        self._tempexcluded = set()

        # This is used when loading a module alias to load any submodule
        self._alias_to_orig = {}

    def find_module(self, fullname, path=None):
        if fullname in self._tempexcluded:
            return None
        if fullname in self._handled or (
            fullname.endswith("._orig") and fullname[:-6] in self._handled
        ):
            return self
        name_parts = fullname.split(".")
        if len(name_parts) > 1 and name_parts[-1] != "_orig":
            # We check if we had an alias created for this module and if so,
            # we are going to load it to properly fully create aliases all
            # the way down.
            parent_name = ".".join(name_parts[:-1])
            if parent_name in self._alias_to_orig:
                return self
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        if not self._can_handle_orig_module() and fullname.endswith("._orig"):
            # We return a nicer error message
            raise ImportError(
                "Attempting to load '%s' -- loading shadowed modules in Metaflow "
                "Extensions are only supported in Python 3.4+" % fullname
            )
        to_import = self._handled.get(fullname, None)

        # If to_import is None, two cases:
        #  - we are loading a ._orig module
        #  - OR we are loading a submodule
        if to_import is None:
            if fullname.endswith("._orig"):
                try:
                    # We exclude this module temporarily from what we handle to
                    # revert back to the non-shadowing mode of import
                    self._tempexcluded.add(fullname)
                    to_import = importlib.util.find_spec(fullname)
                finally:
                    self._tempexcluded.remove(fullname)
            else:
                name_parts = fullname.split(".")
                submodule = name_parts[-1]
                parent_name = ".".join(name_parts[:-1])
                to_import = ".".join([self._alias_to_orig[parent_name], submodule])

        if isinstance(to_import, str):
            try:
                to_import_mod = importlib.import_module(to_import)
            except ImportError:
                raise ImportError(
                    "No module found '%s' (aliasing %s)" % (fullname, to_import)
                )
            sys.modules[fullname] = to_import_mod
            self._alias_to_orig[fullname] = to_import_mod.__name__
        elif isinstance(to_import, types.ModuleType):
            sys.modules[fullname] = to_import
            self._alias_to_orig[fullname] = to_import.__name__
        elif self._can_handle_orig_module() and isinstance(to_import, ModuleSpec):
            # This loads modules that end in _orig
            m = importlib.util.module_from_spec(to_import)
            to_import.loader.exec_module(m)
            sys.modules[fullname] = m
        elif to_import is None and fullname.endswith("._orig"):
            # This happens when trying to access a shadowed ._orig module
            # when actually, there is no shadowed module; print a nicer message
            # Condition is a bit overkill and most likely only checking to_import
            # would be OK. Being extra sure in case _LazyLoader is misused and
            # a None value is passed in.
            raise ImportError(
                "Metaflow Extensions shadowed module '%s' does not exist" % fullname
            )
        else:
            raise ImportError
        return sys.modules[fullname]

    @staticmethod
    def _can_handle_orig_module():
        return sys.version_info[0] >= 3 and sys.version_info[1] >= 4


# We load the module overrides *first* explicitly. Non overrides can be loaded
# in toplevel as well but these can be loaded first if needed. Note that those
# modules should be careful not to include anything in Metaflow at their top-level
# as it is likely to not work.
try:
    import metaflow_extensions.toplevel.module_overrides as extension_module
except ImportError as e:
    ver = sys.version_info[0] * 10 + sys.version_info[1]
    if ver >= 36:
        # e.name is set to the name of the package that fails to load
        # so don't error ONLY IF the error is importing this module (but do
        # error if there is a transitive import error)
        if not (
            isinstance(e, ModuleNotFoundError)
            and e.name
            in [
                "metaflow_extensions",
                "metaflow_extensions.toplevel",
                "metaflow_extensions.toplevel.module_overrides",
            ]
        ):
            print(
                "Cannot load metaflow_extensions top-level configuration -- "
                "if you want to ignore, uninstall metaflow_extensions package"
            )
            raise
else:
    # We load only modules
    lazy_load_custom_modules = {}
    for n, o in extension_module.__dict__.items():
        if (
            isinstance(o, types.ModuleType)
            and o.__package__
            and o.__package__.startswith("metaflow_extensions")
        ):
            lazy_load_custom_modules["metaflow.%s" % n] = o
    if lazy_load_custom_modules:
        # Prepend to make sure extensions package overrides things
        sys.meta_path = [_LazyLoader(lazy_load_custom_modules)] + sys.meta_path

from .event_logger import EventLogger

# Flow spec
from .flowspec import FlowSpec
from .includefile import IncludeFile
from .parameters import Parameter, JSONTypeClass

JSONType = JSONTypeClass()

# current runtime singleton
from .current import current

# data layer
from .datatools import S3

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

# Utilities
from .multicore_utils import parallel_imap_unordered, parallel_map
from .metaflow_profile import profile

# Now override everything other than modules
__version_addl__ = None
try:
    import metaflow_extensions.toplevel.toplevel as extension_module
except ImportError as e:
    ver = sys.version_info[0] * 10 + sys.version_info[1]
    if ver >= 36:
        # e.name is set to the name of the package that fails to load
        # so don't error ONLY IF the error is importing this module (but do
        # error if there is a transitive import error)
        if not (
            isinstance(e, ModuleNotFoundError)
            and e.name
            in [
                "metaflow_extensions",
                "metaflow_extensions.toplevel",
                "metaflow_extensions.toplevel.toplevel",
            ]
        ):
            print(
                "Cannot load metaflow_extensions top-level configuration -- "
                "if you want to ignore, uninstall metaflow_extensions package"
            )
            raise
else:
    # We load into globals whatever we have in extension_module
    # We specifically exclude any modules that may be included (like sys, os, etc)
    # *except* for ones that are part of metaflow_extensions (basically providing
    # an aliasing mechanism)
    lazy_load_custom_modules = {}
    addl_modules = extension_module.__dict__.get("__mf_promote_submodules__")
    if addl_modules:
        # We make an alias for these modules which the metaflow_extensions author
        # wants to expose but that may not be loaded yet
        lazy_load_custom_modules = {
            "metaflow.%s" % k: "metaflow_extensions.%s" % k for k in addl_modules
        }
    for n, o in extension_module.__dict__.items():
        if not n.startswith("__") and not isinstance(o, types.ModuleType):
            globals()[n] = o
        elif (
            isinstance(o, types.ModuleType)
            and o.__package__
            and o.__package__.startswith("metaflow_extensions")
        ):
            lazy_load_custom_modules["metaflow.%s" % n] = o
    if lazy_load_custom_modules:
        # Prepend to make sure custom package overrides things
        sys.meta_path = [_LazyLoader(lazy_load_custom_modules)] + sys.meta_path

    __version_addl__ = getattr(extension_module, "__mf_extensions__", "<unk>")
    if extension_module.__version__:
        __version_addl__ = "%s(%s)" % (__version_addl__, extension_module.__version__)

# Erase all temporary names to avoid leaking things
for _n in [
    "ver",
    "n",
    "o",
    "e",
    "lazy_load_custom_modules",
    "extension_module",
    "addl_modules",
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
