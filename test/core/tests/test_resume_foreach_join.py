"""Resume from a foreach join step re-runs the join."""

from metaflow import FlowSpec, step
from metaflow_test import ResumeFromHere, is_resumed


class ResumeForeachJoinFlow(FlowSpec):
    @step
    def start(self):
        self.arr = [1, 2, 3]
        self.next(self.process, foreach="arr")

    @step
    def process(self):
        self.value = self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.collected = sorted(i.value for i in inputs)
        if not is_resumed():
            raise ResumeFromHere()
        self.collected_after_resume = self.collected
        self.next(self.end)

    @step
    def end(self):
        pass


def test_resume_foreach_join(metaflow_runner, executor):
    first = metaflow_runner(ResumeForeachJoinFlow, executor=executor)
    assert not first.successful

    resumed = metaflow_runner(
        ResumeForeachJoinFlow, executor=executor, resume=True, resume_step="join"
    )
    assert resumed.successful, resumed.stderr
    assert resumed.run()["join"].task.data.collected_after_resume == [1, 2, 3]
