from metaflow_test import MetaflowTest, ExpectationFailed, steps, tag

class CustomizationTest(MetaflowTest):
    """
    Test that the metaflow_custom module is properly loaded
    """
    PRIORITY = 0

    @tag('test_step_decorator')
    @steps(0, ['all'])
    def step_all(self):
        from metaflow.metaflow_config import METAFLOW_ADDITIONAL_VALUE
        self.additional_value = METAFLOW_ADDITIONAL_VALUE

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(step.name, 'additional_value', 42)
            checker.assert_artifact(step.name, 'plugin_set_value', step.name)
