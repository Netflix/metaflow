from metaflow import FlowSpec, step


class StandardFlow(FlowSpec):
    """Standard flow with start/end names — backward compat test."""

    @step
    def start(self):
        self.x = 1
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    StandardFlow()
