"""A simple linear flow using @step(start=True) and @step(end=True) annotations."""

from metaflow import FlowSpec, step, project


@project(name="hello_custom_steps")
class HelloCustomStepsFlow(FlowSpec):
    @step(start=True)
    def begin(self):
        self.message = "Hello from custom start step"
        self.next(self.process)

    @step
    def process(self):
        self.message = self.message + " -> processed"
        self.next(self.finish)

    @step(end=True)
    def finish(self):
        self.result = self.message + " -> done"


if __name__ == "__main__":
    HelloCustomStepsFlow()
