"""Resuming after a failure should re-run the failed step and succeed."""

from metaflow import FlowSpec, step
from metaflow_test import ResumeFromHere, is_resumed


class ResumeEndStepFlow(FlowSpec):
    @step
    def start(self):
        self.data = "start"
        self.next(self.end)

    @step
    def end(self):
        if is_resumed():
            self.data = "foo"
        else:
            self.data = "bar"
            raise ResumeFromHere()


def test_resume_end_step(metaflow_runner, executor):
    # First run fails inside `end` via ResumeFromHere.
    first = metaflow_runner(ResumeEndStepFlow, executor=executor)
    assert not first.successful

    # Resume — same flow class, resume_step="end".
    resumed = metaflow_runner(
        ResumeEndStepFlow, executor=executor, resume=True, resume_step="end"
    )
    assert resumed.successful, resumed.stderr

    run = resumed.run()
    assert run["start"].task.data.data == "start"
    assert run["end"].task.data.data == "foo"
