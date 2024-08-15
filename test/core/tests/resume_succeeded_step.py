from metaflow_test import MetaflowTest, ExpectationFailed, steps


class ResumeSucceededStepTest(MetaflowTest):
    """
    Resuming from the succeeded end step should work
    """

    RESUME = True
    # resuming on a successful step.
    RESUME_STEP = "start"
    PRIORITY = 3
    PARAMETERS = {"int_param": {"default": 123}}

    @steps(0, ["start"])
    def step_start(self):
        if is_resumed():
            self.data = "start_r"
        else:
            self.data = "start"

    @steps(0, ["singleton-end"], required=True)
    def step_end(self):
        if is_resumed():
            self.data = "end_r"
        else:
            self.data = "end"
            raise ResumeFromHere()

    @steps(2, ["all"])
    def step_all(self):
        pass

    def check_results(self, flow, checker):
        for step in flow:
            data_value = step.name + "_r"
            # resumed step will rerun and hence data will have this "_r" suffix.
            if step.name in ["end", "start"]:
                checker.assert_artifact(step.name, "data", data_value)
