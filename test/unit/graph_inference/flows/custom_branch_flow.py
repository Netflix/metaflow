from metaflow import FlowSpec, step


class CustomBranchFlow(FlowSpec):
    """Flow with branches and custom step names."""

    @step(start=True)
    def entry(self):
        self.next(self.a, self.b)

    @step
    def a(self):
        self.val = "a"
        self.next(self.merge)

    @step
    def b(self):
        self.val = "b"
        self.next(self.merge)

    @step
    def merge(self, inputs):
        self.vals = sorted([i.val for i in inputs])
        self.next(self.done)

    @step(end=True)
    def done(self):
        pass


if __name__ == "__main__":
    CustomBranchFlow()
