from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag


class ExtensionsTest(MetaflowTest):
    """
    Test that the metaflow_extensions module is properly loaded
    """

    PRIORITY = 0

    @tag("test_step_decorator")
    @steps(0, ["all"])
    def step_all(self):
        from metaflow.metaflow_config import METAFLOW_ADDITIONAL_VALUE
        from metaflow import tl_value
        from metaflow.plugins.nondecoplugin import my_value

        from metaflow.exception import MetaflowTestException
        from metaflow.plugins.frameworks.pytorch import NewPytorchParallelDecorator

        self.plugin_value = my_value
        self.tl_value = tl_value
        self.additional_value = METAFLOW_ADDITIONAL_VALUE

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(step.name, "additional_value", 42)
            checker.assert_artifact(step.name, "tl_value", 42)
            checker.assert_artifact(step.name, "plugin_value", 42)
            checker.assert_artifact(step.name, "plugin_set_value", step.name)
