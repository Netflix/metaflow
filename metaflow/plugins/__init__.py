try:
    import metaflow_custom.plugins as ext_plugins
except ImportError:
    class _fake(object):
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def get_plugin_cli(self):
            return []

    ext_plugins = _fake(
        FLOW_DECORATORS=[],
        STEP_DECORATORS=[],
        ENVIRONMENTS=[],
        METADATA_PROVIDERS=[],
        SIDECARS={},
        LOGGING_SIDECARS={},
        MONITOR_SIDECARS={})


def get_plugin_cli():
    # it is important that CLIs are not imported when
    # __init__ is imported. CLIs may use e.g.
    # parameters.add_custom_parameters which requires
    # that the flow is imported first

    # Add new CLI commands in this list
    from . import package_cli
    from .aws.batch import batch_cli
    from .aws.step_functions import step_functions_cli

    return ext_plugins.get_plugin_cli() + [
        package_cli.cli,
        batch_cli.cli,
        step_functions_cli.cli]


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
from .retry_decorator import RetryDecorator
from .aws.batch.batch_decorator import BatchDecorator, ResourcesDecorator
from .aws.step_functions.step_functions_decorator import StepFunctionsInternalDecorator
from .conda.conda_step_decorator import CondaStepDecorator

STEP_DECORATORS = _merge_lists([CatchDecorator,
                                TimeoutDecorator,
                                EnvironmentDecorator,
                                ResourcesDecorator,
                                RetryDecorator,
                                BatchDecorator,
                                StepFunctionsInternalDecorator,
                                CondaStepDecorator], ext_plugins.STEP_DECORATORS, 'name')

# Add Conda environment
from .conda.conda_environment import CondaEnvironment
ENVIRONMENTS = _merge_lists([CondaEnvironment], ext_plugins.ENVIRONMENTS, 'TYPE')

# Metadata providers
from .metadata import LocalMetadataProvider, ServiceMetadataProvider

METADATA_PROVIDERS = _merge_lists(
    [LocalMetadataProvider, ServiceMetadataProvider], ext_plugins.METADATA_PROVIDERS, 'TYPE')

# Every entry in this list becomes a class-level flow decorator.
# Add an entry here if you need a new flow-level annotation. Be
# careful with the choice of name though - they become top-level
# imports from the metaflow package.
from .conda.conda_flow_decorator import CondaFlowDecorator
from .aws.step_functions.schedule_decorator import ScheduleDecorator
FLOW_DECORATORS = _merge_lists([CondaFlowDecorator, ScheduleDecorator], ext_plugins.FLOW_DECORATORS, 'name')

from ..mflog.save_logs_periodically import SaveLogsPeriodicallySidecar
# Sidecars
SIDECARS = {'save_logs_periodically': SaveLogsPeriodicallySidecar}
SIDECARS.update(ext_plugins.SIDECARS)

# Add logger
from .debug_logger import DebugEventLogger
LOGGING_SIDECARS = {'debugLogger': DebugEventLogger,
                    'nullSidecarLogger': None}
LOGGING_SIDECARS.update(ext_plugins.LOGGING_SIDECARS)

# Add monitor
from .debug_monitor import DebugMonitor
MONITOR_SIDECARS = {'debugMonitor': DebugMonitor,
                    'nullSidecarMonitor': None}
MONITOR_SIDECARS.update(ext_plugins.MONITOR_SIDECARS)

SIDECARS.update(LOGGING_SIDECARS)
SIDECARS.update(MONITOR_SIDECARS)
