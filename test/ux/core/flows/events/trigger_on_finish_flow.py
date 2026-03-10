"""
A simple flow with @trigger_on_finish that triggers when another flow completes.

When deployed to Argo Workflows with argo-events configured, this flow
creates both a WorkflowTemplate and a Sensor that watches for completion
events from 'HelloFlow'.
"""

import os

from metaflow import FlowSpec, project, step, trigger_on_finish


@trigger_on_finish(flow="HelloFlow")
@project(name="events_test")
class TriggerOnFinishFlow(FlowSpec):
    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.message = "TriggerOnFinishFlow started"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    TriggerOnFinishFlow()
