from metaflow.plugins import add_plugin_support

add_plugin_support(globals())

step_decorator_add("test_step_decorator", ".test_step_decorator", "TestStepDecorator")
flow_decorator_add("test_flow_decorator", ".flow_options", "FlowDecoratorWithOptions")


__mf_promote_submodules__ = ["nondecoplugin", "frameworks"]
