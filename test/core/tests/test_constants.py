"""Class-level constants should be readable inside step bodies."""

from metaflow import FlowSpec, step


class ConstantsFlow(FlowSpec):
    answer = 42

    @step
    def start(self):
        self.observed = self.answer
        self.next(self.end)

    @step
    def end(self):
        assert self.answer == 42


def test_constants(metaflow_runner, executor):
    result = metaflow_runner(ConstantsFlow, executor=executor)
    assert result.successful, result.stderr
    assert result.run()["start"].task.data.observed == 42
