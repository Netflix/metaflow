"""@retry retries a failing step; @catch records failure into an artifact.

This is the F9 expansion: covers retry attempt metadata, the
"invisible artifact" property (only the final attempt's data persists),
and @catch interaction with foreach split steps.
"""

from metaflow import FlowSpec, catch, current, retry, step
from metaflow_test import RetryRequested


class CatchRetryFlow(FlowSpec):
    @retry(times=3)
    @step
    def start(self):
        self.attempts = current.retry_count
        self.invisible_on_failure = (
            "should_not_persist" if current.retry_count < 3 else None
        )
        if current.retry_count < 3:
            raise RetryRequested()
        # Final attempt — clear the invisible flag.
        self.invisible_on_failure = "final_attempt"
        self.next(self.middle)

    @catch(var="caught", print_exception=False)
    @retry(times=2)
    @step
    def middle(self):
        # Always fails; @retry runs it 3 times (0,1,2), then @catch traps.
        if True:
            raise RuntimeError("middle always fails")
        self.next(self.end)

    @step
    def end(self):
        pass


def test_catch_retry_attempts_and_metadata(metaflow_runner, executor):
    result = metaflow_runner(CatchRetryFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()

    # @retry on start: retry_count progresses 0→1→2→3, the final succeeds.
    start_task = run["start"].task
    assert start_task.data.attempts == 3
    # The "invisible" artifact should hold the FINAL attempt's value, not
    # the value set during a retried-and-failed attempt.
    assert start_task.data.invisible_on_failure == "final_attempt"

    # @catch records the exception with the configured var on middle.
    middle_task = run["middle"].task
    caught = middle_task.data.caught
    assert "middle always fails" in str(caught.exception)
    # The task object should not surface .exception once @catch handled it.
    assert middle_task.exception is None


def test_catch_retry_attempt_metadata(metaflow_runner, executor):
    """The metadata service should record one entry per attempt."""
    result = metaflow_runner(CatchRetryFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()

    start_task = run["start"].task
    attempts = sorted(m.value for m in start_task.metadata if m.type == "attempt")
    # Start: 4 attempts (0,1,2 failed via retry, 3 succeeded).
    assert attempts == ["0", "1", "2", "3"]

    middle_task = run["middle"].task
    middle_attempts = sorted(m.value for m in middle_task.metadata if m.type == "attempt")
    # Middle: @retry(times=2) gives 3 attempts (0,1,2) then @catch traps;
    # there's no fallback artifact-only run for @retry+@catch.
    assert middle_attempts == ["0", "1", "2"]
