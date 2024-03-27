from metaflow_test import MetaflowTest, ExpectationFailed, steps


class MergeArtifactsIncludeTest(MetaflowTest):
    PRIORITY = 1

    @steps(0, ["start"])
    def start(self):
        self.non_modified_passdown = "a"
        self.modified_to_same_value = "b"
        self.manual_merge_required = "c"
        self.ignore_me = "d"

    @steps(2, ["linear"])
    def modify_things(self):
        # Set to different things
        from metaflow.metaflow_current import current

        self.manual_merge_required = current.task_id
        self.ignore_me = current.task_id
        self.modified_to_same_value = "e"
        assert_equals(self.non_modified_passdown, "a")

    @steps(0, ["join"], required=True)
    def merge_things(self, inputs):
        from metaflow.metaflow_current import current
        from metaflow.exception import MissingInMergeArtifactsException

        self.manual_merge_required = current.task_id
        # Test to see if we raise an exception if include specifies non-merged things
        assert_exception(
            lambda: self.merge_artifacts(
                inputs, include=["manual_merge_required", "foobar"]
            ),
            MissingInMergeArtifactsException,
        )

        # Test to make sure nothing is set if failed merge_artifacts
        assert not hasattr(self, "non_modified_passdown")

        # Merge include non_modified_passdown
        self.merge_artifacts(inputs, include=["non_modified_passdown"])

        # Ensure that everything we expect is passed down
        assert_equals(self.non_modified_passdown, "a")
        assert_equals(self.manual_merge_required, current.task_id)
        assert not hasattr(self, "ignore_me")
        assert not hasattr(self, "modified_to_same_value")

    @steps(0, ["end"])
    def end(self):
        # Check that all values made it through
        assert_equals(self.non_modified_passdown, "a")
        assert hasattr(self, "manual_merge_required")

    @steps(3, ["all"])
    def step_all(self):
        assert_equals(self.non_modified_passdown, "a")
