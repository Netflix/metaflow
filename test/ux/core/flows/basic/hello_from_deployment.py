"""
Flow for testing DeployedFlow.from_deployment() round-trips.
Uses several parameter types so the from_deployment reconstruction
is exercised with diverse type information.
"""

import time

from metaflow import FlowSpec, Parameter, JSONType, step, project


@project(name="hello_from_deployment")
class HelloFromDeploymentFlow(FlowSpec):
    """Flow for testing from_deployment() with various parameter types."""

    alpha = Parameter("alpha", help="Learning rate", type=float, default=0.01)
    num_epochs = Parameter("num_epochs", type=int, default=10)
    model_type = Parameter("model_type", type=str, default="cnn")
    use_gpu = Parameter("use_gpu", type=bool, default=False)
    config_json = Parameter(
        "config_json", type=JSONType, default='{"layers": [64, 32]}'
    )

    @step
    def start(self):
        self.message = "Metaflow says: Hi!"
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HelloFromDeploymentFlow()
