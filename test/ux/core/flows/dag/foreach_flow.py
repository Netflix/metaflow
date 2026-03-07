from metaflow import FlowSpec, step, project


@project(name="foreach_flow")
class ForeachFlow(FlowSpec):
    @step
    def start(self):
        self.items = [1, 2, 3, 4, 5]
        self.next(self.process, foreach="items")

    @step
    def process(self):
        self.result = self.input * 2
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = sorted([i.result for i in inputs])
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ForeachFlow()
