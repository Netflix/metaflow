from metaflow.extension_support.plugins import process_plugins_description

STEP_DECORATORS_DESC = [
    ("test_step_decorator", ".test_step_decorator.TestStepDecorator"),
]

FLOW_DECORATORS_DESC = [
    ("test_flow_decorator", ".flow_options.FlowDecoratorWithOptions"),
]

process_plugins_description(globals())

__mf_promote_submodules__ = ["nondecoplugin", "frameworks"]
