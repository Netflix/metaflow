"""
Flow with deploy-time trigger: a callable returns the event spec at deploy time.

This tests the DeployTimeField path in the TriggerDecorator.
"""

from metaflow import FlowSpec, Parameter, project, step, trigger


def _get_trigger_spec(ctx):
    """Return event spec at deploy time."""
    return {
        "name": "deploy_time_event",
        "parameters": {"beta": "event_beta"},
    }


@trigger(event=_get_trigger_spec)
@project(name="hello_deploy_time_trigger")
class HelloDeployTimeTriggerFlow(FlowSpec):
    beta = Parameter("beta", type=str, default="default_beta")

    @step
    def start(self):
        self.received_beta = self.beta
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloDeployTimeTriggerFlow()
