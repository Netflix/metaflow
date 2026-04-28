"""@unbounded_foreach: like foreach, no upfront --max-num-splits cap."""

from metaflow import FlowSpec, step


class BasicUnboundedForeachFlow(FlowSpec):
    @step
    def start(self):
        self.arr = ["a", "b", "c"]
        self.next(self.process, foreach="arr")

    @step
    def process(self):
        self.value = self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.values = sorted(i.value for i in inputs)
        assert self.values == ["a", "b", "c"]
        self.next(self.end)

    @step
    def end(self):
        pass


def test_basic_unbounded_foreach(metaflow_runner, executor):
    result = metaflow_runner(BasicUnboundedForeachFlow, executor=executor)
    assert result.successful, result.stderr
    assert result.run()["join"].task.data.values == ["a", "b", "c"]
