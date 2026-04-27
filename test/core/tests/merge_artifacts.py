from metaflow_test import FlowDefinition, ExpectationFailed, steps
import pytest


class MergeArtifacts(FlowDefinition):
    PRIORITY = 1
    SKIP_GRAPHS = [
        "simple_switch",
        "nested_switch",
        "branch_in_switch",
        "foreach_in_switch",
        "switch_in_branch",
        "switch_in_foreach",
        "recursive_switch",
        "recursive_switch_inside_foreach",
    ]

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
        assert self.non_modified_passdown == "a"

    @steps(0, ["join"], required=True)
    def merge_things(self, inputs):
        from metaflow.metaflow_current import current
        from metaflow.exception import (
            UnhandledInMergeArtifactsException,
            MetaflowException,
        )

        # Test to make sure non-merged values are reported
        with pytest.raises(UnhandledInMergeArtifactsException):
            self.merge_artifacts(inputs)

        # Test to make sure nothing is set if failed merge_artifacts
        assert not hasattr(self, "non_modified_passdown")
        assert not hasattr(self, "manual_merge_required")

        # Test to make sure that only one of exclude/include is used
        with pytest.raises(MetaflowException):
            self.merge_artifacts(
                inputs, exclude=["ignore_me"], include=["non_modified_passdown"]
            )

        # Test to make sure nothing is set if failed merge_artifacts
        assert not hasattr(self, "non_modified_passdown")
        assert not hasattr(self, "manual_merge_required")

        # Test actual merge (ignores set values and excluded names, merges common and non modified)
        self.manual_merge_required = current.task_id
        self.merge_artifacts(inputs, exclude=["ignore_me"])

        # Ensure that everything we expect is passed down
        assert self.non_modified_passdown == "a"
        assert self.modified_to_same_value == "e"
        assert self.manual_merge_required == current.task_id
        assert not hasattr(self, "ignore_me")

    @steps(0, ["end"])
    def end(self):
        from metaflow.exception import MetaflowException

        # This is not a join so test exception for calling in non-join
        with pytest.raises(MetaflowException):
            self.merge_artifacts([])
        # Check that all values made it through
        assert self.non_modified_passdown == "a"
        assert self.modified_to_same_value == "e"
        assert hasattr(self, "manual_merge_required")

    @steps(3, ["all"])
    def step_all(self):
        assert self.non_modified_passdown == "a"
