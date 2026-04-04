"""Flow with @timeout(minutes=1) on a step that sleeps 120 seconds.

Used to verify that @timeout(minutes=N) is correctly parsed and enforced —
not silently treated as @timeout(seconds=0).
"""

import time

from metaflow import FlowSpec, step, timeout, project


@project(name="timeout_minutes_flow")
class TimeoutMinutesFlow(FlowSpec):
    """Step with @timeout(minutes=1) sleeps 2 minutes — must be killed after 1 minute."""

    @step
    def start(self):
        self.next(self.slow)

    @timeout(minutes=1)
    @step
    def slow(self):
        # Sleep 2 minutes — should be killed after 1 minute timeout.
        time.sleep(120)
        self.done = True
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TimeoutMinutesFlow()
