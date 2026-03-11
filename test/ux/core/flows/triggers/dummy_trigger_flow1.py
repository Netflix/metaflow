"""Simple upstream flow used as a trigger_on_finish dependency."""

from metaflow import FlowSpec, project, step


@project(name="dummy_trigger_1")
class DummyTriggerFlow1(FlowSpec):
    @step
    def start(self):
        self.value = 42
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DummyTriggerFlow1()
