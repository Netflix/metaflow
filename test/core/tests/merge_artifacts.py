from metaflow_test import MetaflowTest, ExpectationFailed, steps


class MergeArtifactsTest(MetaflowTest):
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
        from metaflow.exception import (
            UnhandledInMergeArtifactsException,
            MetaflowException,
        )

        # Test to make sure non-merged values are reported
        assert_exception(
            lambda: self.merge_artifacts(inputs), UnhandledInMergeArtifactsException
        )

        # Test to make sure nothing is set if failed merge_artifacts
        assert not hasattr(self, "non_modified_passdown")
        assert not hasattr(self, "manual_merge_required")

        # Test to make sure that only one of exclude/include is used
        assert_exception(
            lambda: self.merge_artifacts(
                inputs, exclude=["ignore_me"], include=["non_modified_passdown"]
            ),
            MetaflowException,
        )

        # Test to make sure nothing is set if failed merge_artifacts
        assert not hasattr(self, "non_modified_passdown")
        assert not hasattr(self, "manual_merge_required")

        # Test actual merge (ignores set values and excluded names, merges common and non modified)
        self.manual_merge_required = current.task_id
        self.merge_artifacts(inputs, exclude=["ignore_me"])

        # Ensure that everything we expect is passed down
        assert_equals(self.non_modified_passdown, "a")
        assert_equals(self.modified_to_same_value, "e")
        assert_equals(self.manual_merge_required, current.task_id)
        assert not hasattr(self, "ignore_me")

    @steps(0, ["end"])
    def end(self):
        from metaflow.exception import MetaflowException

        # This is not a join so test exception for calling in non-join
        assert_exception(lambda: self.merge_artifacts([]), MetaflowException)
        # Check that all values made it through
        assert_equals(self.non_modified_passdown, "a")
        assert_equals(self.modified_to_same_value, "e")
        assert hasattr(self, "manual_merge_required")

    @steps(3, ["all"])
    def step_all(self):
        assert_equals(self.non_modified_passdown, "a")
