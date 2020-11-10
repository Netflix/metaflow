def get_plugin_cli():
    return []


from .flow_options import FlowDecoratorWithOptions
FLOW_DECORATORS = [FlowDecoratorWithOptions]

from .test_step_decorator import TestStepDecorator
STEP_DECORATORS = [TestStepDecorator]

ENVIRONMENTS = []

METADATA_PROVIDERS = []

SIDECARS = {}

LOGGING_SIDECARS = {}

MONITOR_SIDECARS = {}
