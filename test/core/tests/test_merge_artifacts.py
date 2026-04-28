"""merge_artifacts pulls inputs into the join step's namespace."""

from metaflow import FlowSpec, step


class MergeArtifactsFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.left, self.right)

    @step
    def left(self):
        self.left_data = "L"
        self.shared = "from-left"
        self.next(self.join)

    @step
    def right(self):
        self.right_data = "R"
        self.shared = "from-left"  # same value on both branches
        self.next(self.join)

    @step
    def join(self, inputs):
        # merge_artifacts pulls non-conflicting artifacts into self.
        self.merge_artifacts(inputs)
        assert self.left_data == "L"
        assert self.right_data == "R"
        assert self.shared == "from-left"
        self.next(self.end)

    @step
    def end(self):
        pass


def test_merge_artifacts(metaflow_runner, executor):
    result = metaflow_runner(MergeArtifactsFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    join_data = run["join"].task.data
    assert join_data.left_data == "L"
    assert join_data.right_data == "R"
    assert join_data.shared == "from-left"
