"""Simple upstream flow used as a trigger dependency."""

from metaflow import FlowSpec, project, step


@project(name="dummy_trigger_2")
class DummyTriggerFlow2(FlowSpec):
    @step
    def start(self):
        self.value = 99
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DummyTriggerFlow2()
