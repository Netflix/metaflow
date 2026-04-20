from metaflow import FlowSpec, step


class FlowAlpha(FlowSpec):
    @step
    def start(self):
        self.val = "alpha"
        self.next(self.end)

    @step
    def end(self):
        pass


class FlowBeta(FlowSpec):
    @step
    def start(self):
        self.val = "beta"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    FlowSpec.main()
