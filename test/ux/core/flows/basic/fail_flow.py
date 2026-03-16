"""Flow that deliberately fails mid-step.

Used to verify that schedulers report FAILED status (not RUNNING or PENDING)
when a flow step raises an unhandled exception.
"""

from metaflow import FlowSpec, step


class FailFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.failing_step)

    @step
    def failing_step(self):
        raise RuntimeError("Deliberate failure for testing scheduler status reporting.")
        self.next(
            self.end
        )  # noqa: unreachable — required for Metaflow's graph validator

    @step
    def end(self):
        pass


if __name__ == "__main__":
    FailFlow()
