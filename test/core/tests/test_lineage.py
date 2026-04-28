"""metaflow client artifact lineage should match the flow's actual data dependencies."""

from metaflow import FlowSpec, step


class LineageFlow(FlowSpec):
    @step
    def start(self):
        self.x = 1
        self.next(self.middle)

    @step
    def middle(self):
        self.y = self.x + 1
        self.next(self.end)

    @step
    def end(self):
        self.z = self.y + 1


def test_lineage(metaflow_runner, executor):
    result = metaflow_runner(LineageFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    assert run["start"].task.data.x == 1
    assert run["middle"].task.data.y == 2
    assert run["end"].task.data.z == 3
