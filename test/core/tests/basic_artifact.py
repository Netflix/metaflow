from metaflow_test import MetaflowTest, ExpectationFailed, steps


class BasicArtifactTest(MetaflowTest):
    """
    Test that an artifact defined in the first step
    is available in all steps downstream.
    """

    PRIORITY = 0

    @steps(0, ["start"])
    def step_start(self):
        self.data = "abc"

    @steps(1, ["join"])
    def step_join(self):
        import metaflow_test

        inputset = {inp.data for inp in inputs}
        assert_equals({"abc"}, inputset)
        self.data = list(inputset)[0]

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        for step in flow:
            checker.assert_artifact(step.name, "data", "abc")
