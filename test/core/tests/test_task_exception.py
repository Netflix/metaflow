"""An uncaught exception in a step should mark the run as failed."""

from metaflow import FlowSpec, step


class TaskExceptionFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.middle)

    @step
    def middle(self):
        raise ValueError("boom from middle")

    @step
    def end(self):
        pass


def test_task_exception_marks_run_failed(metaflow_runner, executor):
    result = metaflow_runner(TaskExceptionFlow, executor=executor)
    assert not result.successful, "expected non-zero rc when a step raises"
    # Run object exists even on failure since metaflow records the failure.
    if result.run_id is not None:
        run = result.run()
        assert not run.successful
