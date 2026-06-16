import time

from metaflow import FlowSpec, step, timeout, project


@project(name="timeout_flow")
class TimeoutFlow(FlowSpec):
    """Verify @timeout decorator does not break normal execution."""

    @step
    def start(self):
        self.next(self.work)

    @timeout(minutes=10)
    @step
    def work(self):
        time.sleep(1)
        self.done = True
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TimeoutFlow()
