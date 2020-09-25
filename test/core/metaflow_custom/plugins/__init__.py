def get_plugin_cli():
    return []


FLOW_DECORATORS = []

from .test_step_decorator import TestStepDecorator
STEP_DECORATORS = [TestStepDecorator]

ENVIRONMENTS = []

SIDECARS = {}

LOGGING_SIDECARS = {}

MONITOR_SIDECARS = {}
