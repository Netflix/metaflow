from metaflow import FlowSpec, step, retry, resources


class SingleStepWithStackedDecosFlow(FlowSpec):
    """Single-step flow with @retry and @resources stacked on top of @step."""

    @retry(times=1)
    @resources(cpu=1, memory=256)
    @step(start=True, end=True)
    def only(self):
        self.v = 42


if __name__ == "__main__":
    SingleStepWithStackedDecosFlow()
