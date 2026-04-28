"""Artifacts created in early steps propagate through merge_artifacts joins."""

from metaflow import FlowSpec, step


class MergeArtifactsPropagationFlow(FlowSpec):
    @step
    def start(self):
        self.early = "from_start"
        self.next(self.left, self.right)

    @step
    def left(self):
        self.left_only = "L"
        self.next(self.join)

    @step
    def right(self):
        self.right_only = "R"
        self.next(self.join)

    @step
    def join(self, inputs):
        self.merge_artifacts(inputs)
        # `early` is shared on both branches (came from start), so merge
        # picks it up automatically.
        assert self.early == "from_start"
        assert self.left_only == "L"
        assert self.right_only == "R"
        self.next(self.end)

    @step
    def end(self):
        # The artifact propagates further.
        assert self.early == "from_start"


def test_merge_artifacts_propagation(metaflow_runner, executor):
    result = metaflow_runner(MergeArtifactsPropagationFlow, executor=executor)
    assert result.successful, result.stderr
    end_data = result.run()["end"].task.data
    assert end_data.early == "from_start"
    assert end_data.left_only == "L"
    assert end_data.right_only == "R"
