import time

from metaflow import FlowSpec, step, timeout, project


@project(name="timeout_enforce_flow")
class TimeoutEnforceFlow(FlowSpec):
    """Step exceeds its @timeout — the run MUST fail."""

    @step
    def start(self):
        self.next(self.slow)

    @timeout(seconds=5)
    @step
    def slow(self):
        # Sleep well beyond the 5-second timeout to guarantee enforcement.
        time.sleep(120)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TimeoutEnforceFlow()
