try:
    import metaflow_custom.plugins as ext_plugins
except ImportError:
    class _fake(object):
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def get_plugin_cli(self):
            return []

    ext_plugins = _fake(
        AUTH_PROVIDERS={},
        FLOW_DECORATORS=[],
        STEP_DECORATORS=[],
        ENVIRONMENTS=[],
        DATA_PROVIDERS={},
        METADATA_PROVIDERS=[],
        SIDECAR={},
        LOGGING_SIDECAR={},
        MONITOR_SIDECAR={})


def get_plugin_cli():
    # it is important that CLIs are not imported when
    # __init__ is imported. CLIs may use e.g.
    # parameters.add_custom_parameters which requires
    # that the flow is imported first

    # Add new CLI commands in this list
    from . import package_cli
    from .aws.batch import batch_cli

    return ext_plugins.get_plugin_cli() + [package_cli.cli, batch_cli.cli]


def _merge_lists(decos, overrides, attr):
    l = list(overrides)
    existing = set([getattr(o, attr) for o in overrides])
    l.extend([d for d in decos if getattr(d, attr) not in existing])
    return l


# Add new decorators in this list
from .catch_decorator import CatchDecorator
from .timeout_decorator import TimeoutDecorator
from .environment_decorator import EnvironmentDecorator
from .retry_decorator import RetryDecorator
from .aws.batch.batch_decorator import BatchDecorator, ResourcesDecorator
from .conda.conda_step_decorator import CondaStepDecorator

STEP_DECORATORS = _merge_lists([
    CatchDecorator,
    TimeoutDecorator,
    EnvironmentDecorator,
    ResourcesDecorator,
    RetryDecorator,
    BatchDecorator,
    CondaStepDecorator], ext_plugins.STEP_DECORATORS, 'name')

# Add Conda environment
from .conda.conda_environment import CondaEnvironment
ENVIRONMENTS = _merge_lists([CondaEnvironment], ext_plugins.ENVIRONMENTS, 'TYPE')

DATA_PROVIDERS = ext_plugins.DATA_PROVIDERS

# Metadata providers
from .metadata import LocalMetadataProvider, ServiceMetadataProvider

METADATA_PROVIDERS = _merge_lists(
    [LocalMetadataProvider, ServiceMetadataProvider], ext_plugins.METADATA_PROVIDERS, 'TYPE')

# Every entry in this list becomes a class-level flow decorator.
# Add an entry here if you need a new flow-level annotation. Be
# careful with the choice of name though - they become top-level
# imports from the metaflow package.
from .conda.conda_flow_decorator import CondaFlowDecorator
FLOW_DECORATORS = _merge_lists([CondaFlowDecorator], ext_plugins.FLOW_DECORATORS, 'name')

# Auth providers
from .aws.aws_client import get_aws_client
AUTH_PROVIDERS = {'aws': get_aws_client}
AUTH_PROVIDERS.update(ext_plugins.AUTH_PROVIDERS)

# Sidecars
SIDECAR = ext_plugins.SIDECAR

# Add logger
from .debug_logger import DebugEventLogger
LOGGING_SIDECAR = {'debugLogger': DebugEventLogger, 
                   'nullSidecarLogger': None}
LOGGING_SIDECAR.update(ext_plugins.LOGGING_SIDECAR)

# Add monitor
from .debug_monitor import DebugMonitor
MONITOR_SIDECAR = {'debugMonitor': DebugMonitor,
                   'nullSidecarMonitor': None}
MONITOR_SIDECAR.update(ext_plugins.MONITOR_SIDECAR)

SIDECAR.update(LOGGING_SIDECAR)
SIDECAR.update(MONITOR_SIDECAR)
