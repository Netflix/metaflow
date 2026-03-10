"""
A simple flow with @trigger that listens for a webhook event.

When deployed to Argo Workflows with argo-events configured, this flow
creates both a WorkflowTemplate and a Sensor that watches for 'test-event'.
The event payload's 'greeting' field maps to the flow's 'greeting' parameter.
"""

import os

from metaflow import FlowSpec, Parameter, project, step, trigger


@trigger(event={"name": "test-event", "parameters": {"greeting": "greeting"}})
@project(name="events_test")
class TriggerFlow(FlowSpec):
    greeting = Parameter("greeting", default="hello from default")

    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.message = "TriggerFlow received: %s" % self.greeting
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TriggerFlow()
