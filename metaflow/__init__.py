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

import types

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

try:
    import metaflow_custom.toplevel as extension_module
except ImportError:
    # We can only distinguish ModuleNotFound versus other issues in Py 3.6+
    # so we ignore everything for now.
    pass
else:
    # We load into globals whatever we have in extension_module
    for n, o in extension_module.__dict__.items():
        if not n.startswith('__') and not isinstance(o, types.ModuleType):
            globals()[n] = o

import pkg_resources
try:
    __version__ = pkg_resources.get_distribution('metaflow').version
except:
    # this happens on remote environments since the job package
    # does not have a version
    __version__ = None
