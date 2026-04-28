"""Resuming from a successful step should reuse the prior task data."""

from metaflow import FlowSpec, step
from metaflow_test import ResumeFromHere, is_resumed


class ResumeSucceededStepFlow(FlowSpec):
    @step
    def start(self):
        self.data = "start"
        self.next(self.middle)

    @step
    def middle(self):
        self.middle_data = "middle"
        self.next(self.end)

    @step
    def end(self):
        if is_resumed():
            assert self.data == "start"
            assert self.middle_data == "middle"
        else:
            raise ResumeFromHere()


def test_resume_succeeded_step(metaflow_runner, executor):
    first = metaflow_runner(ResumeSucceededStepFlow, executor=executor)
    assert not first.successful

    resumed = metaflow_runner(
        ResumeSucceededStepFlow, executor=executor, resume=True, resume_step="middle"
    )
    assert resumed.successful, resumed.stderr
    run = resumed.run()
    # middle is re-run on resume, so it has fresh data; end can read both.
    assert run["end"].task.data.middle_data == "middle"
