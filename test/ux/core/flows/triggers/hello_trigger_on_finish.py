"""
Flow with @trigger_on_finish: triggers when an upstream flow completes.

Used with dummy_trigger_flow1 as the upstream dependency.
"""

from metaflow import FlowSpec, project, step, trigger_on_finish


@trigger_on_finish(flow="DummyTriggerFlow1")
@project(name="hello_trigger_on_finish")
class HelloTriggerOnFinishFlow(FlowSpec):
    @step
    def start(self):
        self.message = "triggered by upstream"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloTriggerOnFinishFlow()
