from metaflow import FlowSpec, step, Config


class SingleStepWithConfigFlow(FlowSpec):
    """Single-step flow wired to a Config, verifies Configs resolve end-to-end."""

    cfg = Config("cfg", default_value={"x": 7})

    @step(start=True, end=True)
    def only(self):
        self.v = self.cfg["x"]


if __name__ == "__main__":
    SingleStepWithConfigFlow()
