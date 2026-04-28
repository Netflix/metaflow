"""@parallel runs the step on N parallel workers (control + workers)."""

from metaflow import FlowSpec, current, parallel, step


class BasicParallelFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.parallel_step, num_parallel=3)

    @parallel
    @step
    def parallel_step(self):
        self.node_index = current.parallel.node_index
        self.next(self.join)

    @step
    def join(self, inputs):
        self.indices = sorted(i.node_index for i in inputs)
        assert self.indices == [0, 1, 2]
        self.next(self.end)

    @step
    def end(self):
        pass


def test_basic_parallel(metaflow_runner, executor):
    result = metaflow_runner(BasicParallelFlow, executor=executor)
    assert result.successful, result.stderr
    assert result.run()["join"].task.data.indices == [0, 1, 2]
