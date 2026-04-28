"""@timeout fails the step if it exceeds the configured wall-clock seconds."""

import time

from metaflow import FlowSpec, catch, step, timeout


class TimeoutDecoratorFlow(FlowSpec):
    @catch(var="caught", print_exception=False)
    @timeout(seconds=2)
    @step
    def start(self):
        # Sleep longer than the timeout — the step should be killed.
        time.sleep(10)
        self.next(self.end)

    @step
    def end(self):
        pass


def test_timeout_decorator(metaflow_runner, executor):
    result = metaflow_runner(TimeoutDecoratorFlow, executor=executor)
    # The flow itself completes (because @catch traps the timeout).
    assert result.successful, result.stderr
    caught = result.run()["start"].task.data.caught
    assert caught is not None
    assert "Timeout" in caught.type or "timeout" in str(caught.exception).lower()
