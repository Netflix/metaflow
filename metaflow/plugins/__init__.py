
def get_plugin_cli():
    # it is important that CLIs are not imported when
    # __init__ is imported. CLIs may use e.g.
    # parameters.add_custom_parameters which requires
    # that the flow is imported first

    # Add new CLI commands in this list
    from . import package_cli
    from .aws.batch import batch_cli
    from .aws.step_functions import step_functions_cli

    return [package_cli.cli,
            batch_cli.cli,
            step_functions_cli.cli]

# Add new decorators in this list
from .catch_decorator import CatchDecorator
from .timeout_decorator import TimeoutDecorator
from .environment_decorator import EnvironmentDecorator
from .retry_decorator import RetryDecorator
from .aws.batch.batch_decorator import BatchDecorator, ResourcesDecorator
from .aws.step_functions.step_functions_decorator import StepFunctionsInternalDecorator
from .conda.conda_step_decorator import CondaStepDecorator

STEP_DECORATORS = [CatchDecorator,
                   TimeoutDecorator,
                   EnvironmentDecorator,
                   ResourcesDecorator,
                   RetryDecorator,
                   BatchDecorator,
                   StepFunctionsInternalDecorator,
                   CondaStepDecorator]

# Add Conda environment
from .conda.conda_environment import CondaEnvironment
ENVIRONMENTS = [CondaEnvironment]


# Every entry in this list becomes a class-level flow decorator.
# Add an entry here if you need a new flow-level annotation. Be
# careful with the choice of name though - they become top-level
# imports from the metaflow package.
from .conda.conda_flow_decorator import CondaFlowDecorator
from .aws.step_functions.schedule_decorator import ScheduleDecorator
FLOW_DECORATORS = [CondaFlowDecorator, ScheduleDecorator]

# Sidecars
SIDECAR = {}

# Add logger
from .debug_logger import DebugEventLogger
LOGGING_SIDECAR = {'debugLogger': DebugEventLogger, 
                   'nullSidecarLogger': None}

# Add monitor
from .debug_monitor import DebugMonitor
MONITOR_SIDECAR = {'debugMonitor': DebugMonitor,
                   'nullSidecarMonitor': None}

SIDECAR.update(LOGGING_SIDECAR)
SIDECAR.update(MONITOR_SIDECAR)