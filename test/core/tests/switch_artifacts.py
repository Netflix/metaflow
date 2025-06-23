from metaflow_test import MetaflowTest, ExpectationFailed, steps


class SwitchArtifactsTest(MetaflowTest):
    PRIORITY = 1

    @steps(0, ["start"])
    def step_start(self):
        # Create various types of artifacts
        self.string_artifact = "test_string"
        self.number_artifact = 42
        self.list_artifact = [1, 2, 3, 4, 5]
        self.dict_artifact = {"key1": "value1", "key2": "value2"}
        self.condition_value = "high"

    @steps(0, ["switch-step"], required=True)
    def step_switch(self):
        # Verify all artifacts from start are available
        assert_equals(self.string_artifact, "test_string")
        assert_equals(self.number_artifact, 42)
        assert_equals(self.list_artifact, [1, 2, 3, 4, 5])
        assert_equals(self.dict_artifact, {"key1": "value1", "key2": "value2"})

        # Modify some artifacts
        self.modified_in_switch = "modified_" + self.string_artifact
        self.list_artifact.append(6)  # Modify list

        # Set condition
        self.condition_result = self.condition_value

        # Use the switch syntax in self.next()
        self.next(
            {"high": self.high_branch, "low": self.low_branch},
            condition="condition_result",
        )

    @steps(0, ["switch-branch-high", "switch-branch-low"], required=True)
    def step_branch(self):
        from metaflow import current

        step_name = current.step_name

        # Verify all artifacts are available in the branch
        assert_equals(self.string_artifact, "test_string")
        assert_equals(self.number_artifact, 42)
        assert_equals(self.list_artifact, [1, 2, 3, 4, 5, 6])  # Modified version
        assert_equals(self.dict_artifact, {"key1": "value1", "key2": "value2"})
        assert_equals(self.modified_in_switch, "modified_test_string")

        # Create branch-specific artifacts
        if step_name == "high_branch":
            self.branch_specific = "high_branch_data"
            self.computed_result = self.number_artifact * 2  # 84
            self.branch_list = [x * 2 for x in self.list_artifact]
        elif step_name == "low_branch":
            self.branch_specific = "low_branch_data"
            self.computed_result = self.number_artifact * 3  # 126
            self.branch_list = [x * 3 for x in self.list_artifact]

    @steps(1, ["all"])
    def step_all(self):
        # Verify artifacts from all previous steps are available
        assert_equals(self.string_artifact, "test_string")
        assert_equals(self.number_artifact, 42)
        assert_equals(self.list_artifact, [1, 2, 3, 4, 5, 6])
        assert_equals(self.dict_artifact, {"key1": "value1", "key2": "value2"})
        assert_equals(self.modified_in_switch, "modified_test_string")

        # Verify branch-specific artifacts
        if hasattr(self, "branch_specific"):
            if self.condition_value == "high":
                assert_equals(self.branch_specific, "high_branch_data")
                assert_equals(self.computed_result, 84)
                assert_equals(self.branch_list, [2, 4, 6, 8, 10, 12])
            else:
                assert_equals(self.branch_specific, "low_branch_data")
                assert_equals(self.computed_result, 126)
                assert_equals(self.branch_list, [3, 6, 9, 12, 15, 18])

    def check_results(self, flow, checker):
        # Verify start step artifacts
        checker.assert_artifact("start", "string_artifact", "test_string")
        checker.assert_artifact("start", "number_artifact", 42)
        checker.assert_artifact("start", "list_artifact", [1, 2, 3, 4, 5])
        checker.assert_artifact(
            "start", "dict_artifact", {"key1": "value1", "key2": "value2"}
        )

        # Verify switch step artifacts
        checker.assert_artifact(
            "switch_step", "modified_in_switch", "modified_test_string"
        )
        checker.assert_artifact("switch_step", "list_artifact", [1, 2, 3, 4, 5, 6])

        # Verify branch artifacts (high branch should execute)
        checker.assert_artifact("high_branch", "branch_specific", "high_branch_data")
        checker.assert_artifact("high_branch", "computed_result", 84)
        checker.assert_artifact("high_branch", "branch_list", [2, 4, 6, 8, 10, 12])

        # Verify end step has all artifacts
        checker.assert_artifact("end", "string_artifact", "test_string")
        checker.assert_artifact("end", "number_artifact", 42)
        checker.assert_artifact("end", "modified_in_switch", "modified_test_string")
        checker.assert_artifact("end", "branch_specific", "high_branch_data")
        checker.assert_artifact("end", "computed_result", 84)
