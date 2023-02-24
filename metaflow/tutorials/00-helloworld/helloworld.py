from metaflow import FlowSpec, step


class HelloFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.hello)

    @step
    def hello(self):
        print("Metaflow says: Hi!")
        self.next(self.end)

    @step
    def end(self):
        print("HelloFlow is all done.")


if __name__ == "__main__":
    HelloFlow()
