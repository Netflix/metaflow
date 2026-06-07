from metaflow import FlowSpec, step


class SingleStepBareFlow(FlowSpec):
    """Single-step flow with a bare @step (no start/end kwargs).

    Verifies that _identify_start_end implicitly treats the only user step
    as both start and end, so the trivial single-step case does not require
    @step(start=True, end=True).
    """

    @step
    def only(self):
        self.x = 42


if __name__ == "__main__":
    SingleStepBareFlow()
