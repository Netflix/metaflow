from metaflow_test import MetaflowTest, ExpectationFailed, steps


class SwitchParametersTest(MetaflowTest):
    PRIORITY = 1
    PARAMETERS = {
        "threshold": {"default": 5, "type": "int"},
        "mode": {"default": "'auto'", "type": "str"},
    }

    @steps(0, ["start"])
    def step_start(self):
        # Use parameters to determine switch condition
        import random

        self.random_value = random.randint(1, 10)
        self.param_threshold = self.threshold
        self.param_mode = self.mode
        self.data = "start_with_params"

    @steps(0, ["switch-step"], required=True)
    def step_switch(self):
        # Use parameter in condition logic
        if self.param_mode == "auto":
            self.condition_result = (
                "high" if self.random_value > self.param_threshold else "low"
            )
        else:
            # Manual mode - always go high
            self.condition_result = "high"

        self.switch_data = f"threshold_{self.param_threshold}_mode_{self.param_mode}"

    @steps(0, ["switch-branch-high", "switch-branch-low"], required=True)
    def step_branch(self):
        from metaflow import current

        step_name = current.step_name

        # Verify we have the parameter values
        assert_equals(self.param_threshold, 5)  # default value
        assert_equals(self.param_mode, "auto")  # default value
        assert_equals(self.data, "start_with_params")

        if step_name == "high_branch":
            self.branch_result = (
                f"high_path_{self.random_value}_gt_{self.param_threshold}"
            )
        elif step_name == "low_branch":
            self.branch_result = (
                f"low_path_{self.random_value}_lte_{self.param_threshold}"
            )

    @steps(1, ["all"])
    def step_all(self):
        # Verify parameter values are preserved
        assert_equals(self.param_threshold, 5)
        assert_equals(self.param_mode, "auto")

        # Verify the branch logic was correct
        if hasattr(self, "branch_result"):
            if self.random_value > self.param_threshold:
                assert self.branch_result.startswith("high_path")
            else:
                assert self.branch_result.startswith("low_path")

    def check_results(self, flow, checker):
        # Verify parameters are accessible
        checker.assert_artifact("start", "param_threshold", 5)
        checker.assert_artifact("start", "param_mode", "auto")

        # Get the random value to determine expected branch
        run = checker.get_run()
        if run is None:
            # CLI checker - just verify basic artifacts exist
            checker.assert_artifact("start", "data", "start_with_params")
        else:
            # Metadata checker - verify correct branch executed
            executed_steps = {step.id for step in run}
            start_task = run["start"].task
            random_value = start_task.data.random_value

            if random_value > 5:  # threshold default
                expected_steps = {"start", "switch_step", "high_branch", "end"}
                assert_equals(executed_steps, expected_steps)
                checker.assert_artifact(
                    "high_branch", "branch_result", f"high_path_{random_value}_gt_5"
                )
            else:
                expected_steps = {"start", "switch_step", "low_branch", "end"}
                assert_equals(executed_steps, expected_steps)
                checker.assert_artifact(
                    "low_branch", "branch_result", f"low_path_{random_value}_lte_5"
                )
