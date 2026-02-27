from metaflow import FlowSpec, step

class BasicFlow(FlowSpec):
    @step
    def start(self):
        self.message = "hello"
        self.next(self.end)

    @step
    def end(self):
        print(self.message)

if __name__ == "__main__":
    BasicFlow()