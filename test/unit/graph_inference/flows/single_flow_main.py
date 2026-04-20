from metaflow import FlowSpec, step


class OnlyFlow(FlowSpec):
    @step
    def start(self):
        self.val = "only"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    FlowSpec.main()
