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


class _LazyLoader(object):
    # This _LazyLoader implements the Importer Protocol defined in PEP 302
    def __init__(self, handled):
        self._handled = handled if handled else {}

    def find_module(self, fullname, path=None):
        if fullname in self._handled:
            return self
        return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        to_import = self._handled.get(fullname, None)
        if isinstance(to_import, str):
            to_import = importlib.import_module(to_import)
            sys.modules[fullname] = to_import
        elif isinstance(to_import, types.ModuleType):
            sys.modules[fullname] = to_import
        else:
            raise ImportError
        return sys.modules[fullname]


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

# Client
from .client import namespace,\
                    get_namespace,\
                    default_namespace,\
                    metadata, \
                    get_metadata, \
                    default_metadata, \
                    Metaflow,\
                    Flow,\
                    Run,\
                    Step,\
                    Task,\
                    DataArtifact

# Utilities
from .multicore_utils import parallel_imap_unordered,\
                             parallel_map
from .metaflow_profile import profile


__version_addl__ = None
try:
    import metaflow_custom.toplevel as extension_module
except ImportError as e:
    ver = sys.version_info[0] * 10 + sys.version_info[1]
    if ver >= 36:
        # e.name is set to the name of the package that fails to load
        # so don't error ONLY IF the error is importing this module (but do
        # error if there is a transitive import error)
        if not (isinstance(e, ModuleNotFoundError) and e.name == 'metaflow_custom'):
            print(
                "Cannot load metaflow_custom top-level configuration -- "
                "if you want to ignore, uninstall metaflow_custom package")
            raise
else:
    # We load into globals whatever we have in extension_module
    # We specifically exclude any modules that may be included (like sys, os, etc)
    # *except* for ones that are part of metaflow_custom (basically providing
    # an aliasing mechanism)
    lazy_load_custom_modules = {}
    addl_modules = extension_module.__dict__.get('__mf_promote_submodules__')
    if addl_modules:
        # We make an alias for these modules which the metaflow_custom author
        # wants to expose but that may not be loaded yet
        lazy_load_custom_modules = {
            'metaflow.%s' % k: 'metaflow_custom.%s' % k for k in addl_modules}
    for n, o in extension_module.__dict__.items():
        if not n.startswith('__') and not isinstance(o, types.ModuleType):
            globals()[n] = o
        elif isinstance(o, types.ModuleType) and o.__package__ and \
                o.__package__.startswith('metaflow_custom'):
            lazy_load_custom_modules['metaflow.%s' % n] = o
    if lazy_load_custom_modules:
        # Prepend to make sure custom package overrides things
        sys.meta_path = [_LazyLoader(lazy_load_custom_modules)] + sys.meta_path
    __version_addl__ = getattr(extension_module, '__mf_customization__', '<unk>')
    if extension_module.__version__:
        __version_addl__ = '%s(%s)' % (__version_addl__, extension_module.__version__)
finally:
    # Erase all temporary names to avoid leaking things
    for _n in ['ver', 'n', 'o', 'e', 'lazy_load_custom_modules',
               'extension_module', 'addl_modules']:
        try:
            del globals()[_n]
        except KeyError:
            pass
    del globals()['_n']

import pkg_resources
try:
    __version__ = pkg_resources.get_distribution('metaflow').version
except:
    # this happens on remote environments since the job package
    # does not have a version
    __version__ = None
