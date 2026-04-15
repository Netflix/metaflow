from metaflow import FlowSpec, step


class SingleStepFlow(FlowSpec):
    """Flow with a single step to test start==end inference."""

    @step(start=True, end=True)
    def only(self):
        self.x = 42


if __name__ == "__main__":
    SingleStepFlow()
