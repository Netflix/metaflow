"""Resume re-runs all foreach branches when resuming from the split step."""

from metaflow import FlowSpec, step
from metaflow_test import ResumeFromHere, is_resumed


class ResumeForeachSplitFlow(FlowSpec):
    @step
    def start(self):
        self.arr = [1, 2, 3, 4]
        if not is_resumed():
            raise ResumeFromHere()
        self.next(self.process, foreach="arr")

    @step
    def process(self):
        self.value = self.input
        self.next(self.join)

    @step
    def join(self, inputs):
        self.collected = sorted(i.value for i in inputs)
        assert self.collected == [1, 2, 3, 4]
        self.next(self.end)

    @step
    def end(self):
        pass


def test_resume_foreach_split(metaflow_runner, executor):
    first = metaflow_runner(ResumeForeachSplitFlow, executor=executor)
    assert not first.successful

    resumed = metaflow_runner(
        ResumeForeachSplitFlow, executor=executor, resume=True, resume_step="start"
    )
    assert resumed.successful, resumed.stderr
    assert resumed.run()["join"].task.data.collected == [1, 2, 3, 4]
