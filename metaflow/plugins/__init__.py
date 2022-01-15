import sys
import types

_expected_extensions = {
    "FLOW_DECORATORS": [],
    "STEP_DECORATORS": [],
    "ENVIRONMENTS": [],
    "METADATA_PROVIDERS": [],
    "SIDECARS": {},
    "LOGGING_SIDECARS": {},
    "MONITOR_SIDECARS": {},
    "AWS_CLIENT_PROVIDERS": [],
    "get_plugin_cli": lambda: [],
}

try:
    import metaflow_extensions.plugins as _ext_plugins
except ImportError as e:
    ver = sys.version_info[0] * 10 + sys.version_info[1]
    if ver >= 36:
        # e.name is set to the name of the package that fails to load
        # so don't error ONLY IF the error is importing this module (but do
        # error if there is a transitive import error)
        if not (
            isinstance(e, ModuleNotFoundError)
            and e.name in ["metaflow_extensions", "metaflow_extensions.plugins"]
        ):
            print(
                "Cannot load metaflow_extensions plugins -- "
                "if you want to ignore, uninstall metaflow_extensions package"
            )
            raise

    class _fake(object):
        def __getattr__(self, name):
            if name in _expected_extensions:
                return _expected_extensions[name]
            raise AttributeError

    _ext_plugins = _fake()
else:
    # We load into globals whatever we have in extension_module
    # We specifically exclude any modules that may be included (like sys, os, etc)
    # *except* for ones that are part of metaflow_extensions (basically providing
    # an aliasing mechanism)
    lazy_load_custom_modules = {}
    addl_modules = _ext_plugins.__dict__.get("__mf_promote_submodules__")
    if addl_modules:
        # We make an alias for these modules which the metaflow_extensions author
        # wants to expose but that may not be loaded yet
        lazy_load_custom_modules = {
            "metaflow.plugins.%s" % k: "metaflow_extensions.plugins.%s" % k
            for k in addl_modules
        }
    for n, o in _ext_plugins.__dict__.items():
        if not n.startswith("__") and not isinstance(o, types.ModuleType):
            globals()[n] = o
        elif (
            isinstance(o, types.ModuleType)
            and o.__package__
            and o.__package__.startswith("metaflow_extensions")
        ):
            lazy_load_custom_modules["metaflow.plugins.%s" % n] = o
    if lazy_load_custom_modules:
        # NOTE: We load things first to have metaflow_extensions override things here.
        # This does mean that for modules that have the same name (for example,
        # if metaflow_extensions.plugins also provides a conda module), it needs
        # to provide whatever is expected below (so for example a `conda_step_decorator`
        # file with a `CondaStepDecorator` class).
        # We do this because we want metaflow_extensions to fully override things
        # and if we did not change sys.meta_path here, the lines below would
        # load the non metaflow_extensions modules providing for possible confusion.
        # This keeps it cleaner.
        from metaflow import _LazyLoader

        sys.meta_path = [_LazyLoader(lazy_load_custom_modules)] + sys.meta_path

    class _wrap(object):
        def __init__(self, obj):
            self.__dict__ = obj.__dict__

        def __getattr__(self, name):
            if name in _expected_extensions:
                return _expected_extensions[name]
            raise AttributeError

    _ext_plugins = _wrap(_ext_plugins)


def get_plugin_cli():
    # it is important that CLIs are not imported when
    # __init__ is imported. CLIs may use e.g.
    # parameters.add_custom_parameters which requires
    # that the flow is imported first

    # Add new CLI commands in this list
    from . import package_cli
    from .aws.batch import batch_cli
    from .aws.eks import kubernetes_cli
    from .aws.step_functions import step_functions_cli
    from .cards import card_cli

    return _ext_plugins.get_plugin_cli() + [
        package_cli.cli,
        batch_cli.cli,
        card_cli.cli,
        kubernetes_cli.cli,
        step_functions_cli.cli,
    ]


def _merge_lists(base, overrides, attr):
    # Merge two lists of classes by comparing them for equality using 'attr'.
    # This function prefers anything in 'overrides'. In other words, if a class
    # is present in overrides and matches (according to the equality criterion) a class in
    # base, it will be used instead of the one in base.
    l = list(overrides)
    existing = set([getattr(o, attr) for o in overrides])
    l.extend([d for d in base if getattr(d, attr) not in existing])
    return l


# Add new decorators in this list
from .catch_decorator import CatchDecorator
from .timeout_decorator import TimeoutDecorator
from .environment_decorator import EnvironmentDecorator
from .parallel_decorator import ParallelDecorator
from .retry_decorator import RetryDecorator
from .resources_decorator import ResourcesDecorator
from .aws.batch.batch_decorator import BatchDecorator
from .aws.eks.kubernetes_decorator import KubernetesDecorator
from .aws.step_functions.step_functions_decorator import StepFunctionsInternalDecorator
from .test_unbounded_foreach_decorator import (
    InternalTestUnboundedForeachDecorator,
    InternalTestUnboundedForeachInput,
)
from .conda.conda_step_decorator import CondaStepDecorator
from .cards.card_decorator import CardDecorator
from .frameworks.pytorch import PytorchParallelDecorator


STEP_DECORATORS = _merge_lists(
    [
        CatchDecorator,
        TimeoutDecorator,
        EnvironmentDecorator,
        ResourcesDecorator,
        RetryDecorator,
        BatchDecorator,
        CardDecorator,
        KubernetesDecorator,
        StepFunctionsInternalDecorator,
        CondaStepDecorator,
        ParallelDecorator,
        PytorchParallelDecorator,
        InternalTestUnboundedForeachDecorator,
    ],
    _ext_plugins.STEP_DECORATORS,
    "name",
)

# Add Conda environment
from .conda.conda_environment import CondaEnvironment

ENVIRONMENTS = _merge_lists([CondaEnvironment], _ext_plugins.ENVIRONMENTS, "TYPE")

# Metadata providers
from .metadata import LocalMetadataProvider, ServiceMetadataProvider

METADATA_PROVIDERS = _merge_lists(
    [LocalMetadataProvider, ServiceMetadataProvider],
    _ext_plugins.METADATA_PROVIDERS,
    "TYPE",
)

# Every entry in this list becomes a class-level flow decorator.
# Add an entry here if you need a new flow-level annotation. Be
# careful with the choice of name though - they become top-level
# imports from the metaflow package.
from .conda.conda_flow_decorator import CondaFlowDecorator
from .aws.step_functions.schedule_decorator import ScheduleDecorator
from .project_decorator import ProjectDecorator

FLOW_DECORATORS = _merge_lists(
    [CondaFlowDecorator, ScheduleDecorator, ProjectDecorator],
    _ext_plugins.FLOW_DECORATORS,
    "name",
)

# Cards
from .cards.card_modules.basic import DefaultCard, TaskSpecCard, ErrorCard
from .cards.card_modules.test_cards import TestErrorCard, TestTimeoutCard, TestMockCard

CARDS = [
    DefaultCard,
    TaskSpecCard,
    ErrorCard,
    TestErrorCard,
    TestTimeoutCard,
    TestMockCard,
]
# Sidecars
from ..mflog.save_logs_periodically import SaveLogsPeriodicallySidecar
from metaflow.metadata.heartbeat import MetadataHeartBeat

SIDECARS = {
    "save_logs_periodically": SaveLogsPeriodicallySidecar,
    "heartbeat": MetadataHeartBeat,
}
SIDECARS.update(_ext_plugins.SIDECARS)

# Add logger
from .debug_logger import DebugEventLogger

LOGGING_SIDECARS = {"debugLogger": DebugEventLogger, "nullSidecarLogger": None}
LOGGING_SIDECARS.update(_ext_plugins.LOGGING_SIDECARS)

# Add monitor
from .debug_monitor import DebugMonitor

MONITOR_SIDECARS = {"debugMonitor": DebugMonitor, "nullSidecarMonitor": None}
MONITOR_SIDECARS.update(_ext_plugins.MONITOR_SIDECARS)

SIDECARS.update(LOGGING_SIDECARS)
SIDECARS.update(MONITOR_SIDECARS)

from .aws.aws_client import Boto3ClientProvider

AWS_CLIENT_PROVIDERS = _merge_lists(
    [Boto3ClientProvider], _ext_plugins.AWS_CLIENT_PROVIDERS, "name"
)

# Erase all temporary names to avoid leaking things
# We leave '_ext_plugins' and '_expected_extensions' because they are used in
# a function (so they need to stick around)
for _n in [
    "ver",
    "n",
    "o",
    "e",
    "lazy_load_custom_modules",
    "_LazyLoader",
    "_merge_lists",
    "_fake",
    "_wrap",
    "addl_modules",
]:
    try:
        del globals()[_n]
    except KeyError:
        pass
del globals()["_n"]
