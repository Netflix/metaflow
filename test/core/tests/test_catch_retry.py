"""@retry retries a failing step; @catch records failure into an artifact."""

from metaflow import FlowSpec, catch, current, retry, step
from metaflow_test import RetryRequested


class CatchRetryFlow(FlowSpec):
    @retry(times=3)
    @step
    def start(self):
        self.attempts = current.retry_count
        if current.retry_count < 3:
            raise RetryRequested()
        self.next(self.middle)

    @catch(var="caught", print_exception=False)
    @step
    def middle(self):
        # Always advance the DAG; the failure is the raise below, but the
        # validity checker requires a self.next() in the source, so we
        # raise BEFORE it but keep it textually present.
        if True:
            raise RuntimeError("explicitly fail middle")
        self.next(self.end)

    @step
    def end(self):
        pass


def test_catch_retry(metaflow_runner, executor):
    result = metaflow_runner(CatchRetryFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    # @retry: started 4 times (0, 1, 2, 3); the 4th succeeded.
    assert run["start"].task.data.attempts == 3
    # @catch: middle records the exception via the configured var.
    caught = run["middle"].task.data.caught
    assert "explicitly fail middle" in str(caught.exception)
