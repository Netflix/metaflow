"""Resuming inside a foreach: re-runs all foreach branches from a step."""

from metaflow import FlowSpec, step
from metaflow_test import ResumeFromHere, is_resumed


class ResumeForeachInnerFlow(FlowSpec):
    @step
    def start(self):
        self.arr = [1, 2, 3]
        self.next(self.process, foreach="arr")

    @step
    def process(self):
        self.value = self.input
        if not is_resumed():
            raise ResumeFromHere()
        self.value_after_resume = self.value * 10
        self.next(self.join)

    @step
    def join(self, inputs):
        self.collected = sorted(i.value_after_resume for i in inputs)
        assert self.collected == [10, 20, 30]
        self.next(self.end)

    @step
    def end(self):
        pass


def test_resume_foreach_inner(metaflow_runner, executor):
    first = metaflow_runner(ResumeForeachInnerFlow, executor=executor)
    assert not first.successful

    resumed = metaflow_runner(
        ResumeForeachInnerFlow, executor=executor, resume=True, resume_step="process"
    )
    assert resumed.successful, resumed.stderr
    run = resumed.run()
    assert run["join"].task.data.collected == [10, 20, 30]
