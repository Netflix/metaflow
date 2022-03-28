from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ResumeEndStepTest(MetaflowTest):
    """
    Resuming from the end step should work
    """

    RESUME = True
    PRIORITY = 3
    PARAMETERS = {"int_param": {"default": 123}}

    @steps(0, ["start"])
    def step_start(self):
        self.data = "start"

    @steps(0, ["singleton-end"], required=True)
    def step_end(self):
        if is_resumed():
            self.data = "foo"
        else:
            self.data = "bar"
            raise ResumeFromHere()

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        for step in flow:
            if step.name == "end":
                checker.assert_artifact(step.name, "data", "foo")
            else:
                checker.assert_artifact(step.name, "data", "start")
