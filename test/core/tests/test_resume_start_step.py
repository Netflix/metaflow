"""Resuming from the start step should re-execute it."""

from metaflow import FlowSpec, step
from metaflow_test import ResumeFromHere, is_resumed


class ResumeStartStepFlow(FlowSpec):
    @step
    def start(self):
        if is_resumed():
            self.data = "resumed"
        else:
            self.data = "first"
            raise ResumeFromHere()
        self.next(self.end)

    @step
    def end(self):
        pass


def test_resume_start_step(metaflow_runner, executor):
    first = metaflow_runner(ResumeStartStepFlow, executor=executor)
    assert not first.successful

    resumed = metaflow_runner(
        ResumeStartStepFlow, executor=executor, resume=True, resume_step="start"
    )
    assert resumed.successful, resumed.stderr
    run = resumed.run()
    assert run["start"].task.data.data == "resumed"
