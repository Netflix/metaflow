import sys
import traceback
import types


def _merge_lists(base, overrides, attr):
    # Merge two lists of classes by comparing them for equality using 'attr'.
    # This function prefers anything in 'overrides'. In other words, if a class
    # is present in overrides and matches (according to the equality criterion) a class in
    # base, it will be used instead of the one in base.
    l = list(overrides)
    existing = set([getattr(o, attr) for o in overrides])
    l.extend([d for d in base if getattr(d, attr) not in existing])
    base[:] = l[:]


def _merge_funcs(base_func, override_func):
    # IMPORTANT: This is a `get_plugin_cli` type of function, and we need to *delay*
    # evaluation of it until after the flowspec is loaded.
    old_default = base_func.__defaults__[0]
    r = lambda: base_func(old_default) + override_func()

    base_func.__defaults__ = (r,)


_expected_extensions = {
    "FLOW_DECORATORS": (
        [],
        lambda base, overrides: _merge_lists(base, overrides, "name"),
    ),
    "STEP_DECORATORS": (
        [],
        lambda base, overrides: _merge_lists(base, overrides, "name"),
    ),
    "ENVIRONMENTS": (
        [],
        lambda base, overrides: _merge_lists(base, overrides, "TYPE"),
    ),
    "METADATA_PROVIDERS": (
        [],
        lambda base, overrides: _merge_lists(base, overrides, "TYPE"),
    ),
    "SIDECARS": ({}, lambda base, overrides: base.update(overrides)),
    "LOGGING_SIDECARS": ({}, lambda base, overrides: base.update(overrides)),
    "MONITOR_SIDECARS": ({}, lambda base, overrides: base.update(overrides)),
    "AWS_CLIENT_PROVIDERS": (
        [],
        lambda base, overrides: _merge_lists(base, overrides, "name"),
    ),
    "get_plugin_cli": (lambda l=None: [] if l is None else l(), _merge_funcs),
}


try:
    from metaflow.extension_support import get_modules, multiload_all, _ext_debug

    _modules_to_import = get_modules("plugins")

    multiload_all(_modules_to_import, "plugins", globals())

    # Build an ordered list
    _ext_plugins = {k: v[0] for k, v in _expected_extensions.items()}
    for m in _modules_to_import:
        for k, v in _expected_extensions.items():
            module_override = m.module.__dict__.get(k)
            if module_override is not None:
                v[1](_ext_plugins[k], module_override)
except Exception as e:
    _ext_debug("\tWARNING: ignoring all plugins due to error during import: %s" % e)
    print(
        "WARNING: Plugins did not load -- ignoring all of them which may not "
        "be what you want: %s" % e
    )
    traceback.print_exc()
    _ext_plugins = {k: v[0] for k, v in _expected_extensions.items()}

_ext_debug("\tWill import the following plugins: %s" % str(_ext_plugins))


def get_plugin_cli():
    # it is important that CLIs are not imported when
    # __init__ is imported. CLIs may use e.g.
    # parameters.add_custom_parameters which requires
    # that the flow is imported first

    # Add new CLI commands in this list
    from . import package_cli
    from .aws.batch import batch_cli
    from .kubernetes import kubernetes_cli
    from .aws.step_functions import step_functions_cli
    from .airflow import airflow_cli
    from .argo import argo_workflows_cli
    from .cards import card_cli
    from . import tag_cli

    return _ext_plugins["get_plugin_cli"]() + [
        package_cli.cli,
        batch_cli.cli,
        card_cli.cli,
        kubernetes_cli.cli,
        step_functions_cli.cli,
        airflow_cli.cli,
        argo_workflows_cli.cli,
        tag_cli.cli,
    ]


# Add new decorators in this list
from .catch_decorator import CatchDecorator
from .timeout_decorator import TimeoutDecorator
from .environment_decorator import EnvironmentDecorator
from .parallel_decorator import ParallelDecorator
from .retry_decorator import RetryDecorator
from .resources_decorator import ResourcesDecorator
from .aws.batch.batch_decorator import BatchDecorator
from .kubernetes.kubernetes_decorator import KubernetesDecorator
from .argo.argo_workflows_decorator import ArgoWorkflowsInternalDecorator
from .aws.step_functions.step_functions_decorator import StepFunctionsInternalDecorator
from .test_unbounded_foreach_decorator import (
    InternalTestUnboundedForeachDecorator,
    InternalTestUnboundedForeachInput,
)
from .conda.conda_step_decorator import CondaStepDecorator
from .cards.card_decorator import CardDecorator
from .frameworks.pytorch import PytorchParallelDecorator
from .airflow.airflow_decorator import AirflowInternalDecorator


STEP_DECORATORS = [
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
    AirflowInternalDecorator,
    ArgoWorkflowsInternalDecorator,
]
_merge_lists(STEP_DECORATORS, _ext_plugins["STEP_DECORATORS"], "name")

# Add Conda environment
from .conda.conda_environment import CondaEnvironment

ENVIRONMENTS = [CondaEnvironment]
_merge_lists(ENVIRONMENTS, _ext_plugins["ENVIRONMENTS"], "TYPE")

# Metadata providers
from .metadata import LocalMetadataProvider, ServiceMetadataProvider

METADATA_PROVIDERS = [LocalMetadataProvider, ServiceMetadataProvider]
_merge_lists(METADATA_PROVIDERS, _ext_plugins["METADATA_PROVIDERS"], "TYPE")

# Every entry in this list becomes a class-level flow decorator.
# Add an entry here if you need a new flow-level annotation. Be
# careful with the choice of name though - they become top-level
# imports from the metaflow package.
from .conda.conda_flow_decorator import CondaFlowDecorator
from .aws.step_functions.schedule_decorator import ScheduleDecorator
from .project_decorator import ProjectDecorator


FLOW_DECORATORS = [
    CondaFlowDecorator,
    ScheduleDecorator,
    ProjectDecorator,
]
_merge_lists(FLOW_DECORATORS, _ext_plugins["FLOW_DECORATORS"], "name")

# Cards
from .cards.card_modules.basic import (
    DefaultCard,
    TaskSpecCard,
    ErrorCard,
    BlankCard,
    DefaultCardJSON,
)
from .cards.card_modules.test_cards import (
    TestErrorCard,
    TestTimeoutCard,
    TestMockCard,
    TestPathSpecCard,
    TestEditableCard,
    TestEditableCard2,
    TestNonEditableCard,
)
from .cards.card_modules import MF_EXTERNAL_CARDS

CARDS = [
    DefaultCard,
    TaskSpecCard,
    ErrorCard,
    BlankCard,
    TestErrorCard,
    TestTimeoutCard,
    TestMockCard,
    TestPathSpecCard,
    TestEditableCard,
    TestEditableCard2,
    TestNonEditableCard,
    BlankCard,
    DefaultCardJSON,
]
_merge_lists(CARDS, MF_EXTERNAL_CARDS, "type")
# Sidecars
from ..mflog.save_logs_periodically import SaveLogsPeriodicallySidecar
from metaflow.metadata.heartbeat import MetadataHeartBeat

SIDECARS = {
    "save_logs_periodically": SaveLogsPeriodicallySidecar,
    "heartbeat": MetadataHeartBeat,
}
SIDECARS.update(_ext_plugins["SIDECARS"])

# Add logger
from .debug_logger import DebugEventLogger
from metaflow.event_logger import NullEventLogger

LOGGING_SIDECARS = {
    DebugEventLogger.TYPE: DebugEventLogger,
    NullEventLogger.TYPE: NullEventLogger,
}
LOGGING_SIDECARS.update(_ext_plugins["LOGGING_SIDECARS"])

# Add monitor
from .debug_monitor import DebugMonitor
from metaflow.monitor import NullMonitor

MONITOR_SIDECARS = {
    DebugMonitor.TYPE: DebugMonitor,
    NullMonitor.TYPE: NullMonitor,
}
MONITOR_SIDECARS.update(_ext_plugins["MONITOR_SIDECARS"])

SIDECARS.update(LOGGING_SIDECARS)
SIDECARS.update(MONITOR_SIDECARS)

from .aws.aws_client import Boto3ClientProvider

AWS_CLIENT_PROVIDERS = [Boto3ClientProvider]
_merge_lists(AWS_CLIENT_PROVIDERS, _ext_plugins["AWS_CLIENT_PROVIDERS"], "name")


# Erase all temporary names to avoid leaking things
# We leave '_ext_plugins' because it is used in
# a function (so it needs to stick around)
for _n in [
    "_merge_lists",
    "_merge_funcs",
    "_expected_extensions",
    "get_modules",
    "multiload_all",
    "_modules_to_import",
    "k",
    "v",
    "module_override",
]:
    try:
        del globals()[_n]
    except KeyError:
        pass
del globals()["_n"]
