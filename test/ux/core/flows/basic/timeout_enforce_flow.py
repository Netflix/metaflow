import time

from metaflow import FlowSpec, step, timeout, project


@project(name="timeout_enforce_flow")
class TimeoutEnforceFlow(FlowSpec):
    """Step exceeds its @timeout — the run MUST fail."""

    @step
    def start(self):
        self.next(self.slow)

    @timeout(seconds=60)
    @step
    def slow(self):
        # Sleep well beyond the 60-second timeout to guarantee enforcement.
        # Kubernetes requires at least 60s for @timeout.
        time.sleep(300)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TimeoutEnforceFlow()
