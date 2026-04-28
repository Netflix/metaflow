"""An artifact set in start should be visible in every downstream step."""

from metaflow import FlowSpec, step


class BasicArtifactFlow(FlowSpec):
    @step
    def start(self):
        self.data = "abc"
        self.next(self.middle)

    @step
    def middle(self):
        assert self.data == "abc"
        self.next(self.end)

    @step
    def end(self):
        assert self.data == "abc"


def test_basic_artifact(metaflow_runner, executor):
    result = metaflow_runner(BasicArtifactFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    for step_name in ("start", "middle", "end"):
        for task in run[step_name]:
            assert task.data.data == "abc"
