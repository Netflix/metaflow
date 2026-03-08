from metaflow import FlowSpec, step, catch, project


@project(name="catch_flow")
class CatchFlow(FlowSpec):
    """Verify @catch stores the exception and lets the flow continue."""

    @step
    def start(self):
        self.next(self.failing)

    @catch(var="error")
    @step
    def failing(self):
        raise ValueError("Intentional error for @catch test")
        self.next(self.end)  # noqa: unreachable — required by Metaflow validator

    @step
    def end(self):
        pass


if __name__ == "__main__":
    CatchFlow()
