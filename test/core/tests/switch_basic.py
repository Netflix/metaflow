from metaflow_test import MetaflowTest, ExpectationFailed, steps


class BasicSwitchTest(MetaflowTest):
    PRIORITY = 0

    @steps(0, ["start"])
    def step_start(self):
        self.switch_value = "high"
        self.data = "start_data"

    @steps(0, ["switch-step"], required=True)
    def step_switch(self):
        # Set the condition that determines which branch to take
        self.condition_result = self.switch_value
        self.switch_data = "from_switch"
        self.next(
            {"high": self.high_branch, "low": self.low_branch},
            condition="condition_result",
        )

    @steps(0, ["switch-branch-high", "switch-branch-low"], required=True)
    def step_branch(self):
        from metaflow import current

        step_name = current.step_name

        # Verify we have the correct data from previous steps
        assert_equals(self.data, "start_data")
        assert_equals(self.switch_data, "from_switch")

        if step_name == "high_branch":
            assert_equals(self.switch_value, "high")
            self.branch_result = "high_path_executed"
        elif step_name == "low_branch":
            assert_equals(self.switch_value, "low")
            self.branch_result = "low_path_executed"

    @steps(1, ["all"])
    def step_all(self):
        # Verify we have the correct branch result
        if hasattr(self, "branch_result"):
            if self.switch_value == "high":
                assert_equals(self.branch_result, "high_path_executed")
            else:
                assert_equals(self.branch_result, "low_path_executed")

    def check_results(self, flow, checker):
        # Verify start step artifacts
        checker.assert_artifact("start", "switch_value", "high")
        checker.assert_artifact("start", "data", "start_data")

        # Verify switch step artifacts
        checker.assert_artifact("switch_step", "condition_result", "high")
        checker.assert_artifact("switch_step", "switch_data", "from_switch")

        # Verify only the high branch executed
        checker.assert_artifact("high_branch", "branch_result", "high_path_executed")

        # Verify the low branch did not execute (should not have artifacts)
        try:
            artifacts = checker.artifact_dict_if_exists("low_branch", "branch_result")
            # If low_branch exists, it means the switch didn't work correctly
            if artifacts and any(artifacts.values()):
                raise ExpectationFailed(
                    "low_branch should not have executed", "low_branch executed"
                )
        except:
            # Expected - low_branch should not exist
            pass

        # Verify end step has correct data
        checker.assert_artifact("end", "switch_value", "high")
        checker.assert_artifact("end", "data", "start_data")
