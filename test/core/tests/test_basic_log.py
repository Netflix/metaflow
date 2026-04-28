"""stdout / stderr written from a step should be retrievable via task.stdout/stderr."""

from metaflow import FlowSpec, step


class BasicLogFlow(FlowSpec):
    @step
    def start(self):
        import sys

        sys.stdout.write("stdout payload\n")
        sys.stderr.write("stderr payload\n")
        self.next(self.end)

    @step
    def end(self):
        pass


def test_basic_log(metaflow_runner, executor):
    result = metaflow_runner(BasicLogFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    start_task = run["start"].task
    assert "stdout payload" in start_task.stdout
    assert "stderr payload" in start_task.stderr
