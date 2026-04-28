"""Foreach with many splits stresses task ordering / client APIs."""

from metaflow import FlowSpec, step


N = 1200


class WideForeachFlow(FlowSpec):
    @step
    def start(self):
        self.arr = list(range(N))
        self.next(self.process, foreach="arr")

    @step
    def process(self):
        self.value = self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.collected = sorted(i.value for i in inputs)
        assert self.collected == list(range(N))
        self.next(self.end)

    @step
    def end(self):
        pass


def test_wide_foreach(metaflow_runner, executor):
    result = metaflow_runner(WideForeachFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    # Iterating many tasks should still work.
    process_values = sorted(t.data.value for t in run["process"])
    assert process_values == list(range(N))
