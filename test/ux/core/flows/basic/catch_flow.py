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
        # Wrap in a condition so self.next() is always reachable for the graph
        # validator, regardless of which version of Metaflow is installed.
        if True:
            raise ValueError("Intentional error for @catch test")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    CatchFlow()
