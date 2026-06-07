import os

from metaflow import FlowSpec, step, project


@project(name="multi_body_foreach_flow")
class MultibodyForeachFlow(FlowSpec):
    """Foreach with two linear body steps before the join.

    Topology: start(foreach) -> process -> transform -> join -> end

    This exercises the case where the foreach body has more than one step,
    which requires the orchestrator to thread task IDs sequentially through
    intermediate body steps rather than treating the join as the direct
    successor of a single body step.
    """

    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.items = [1, 2, 3]
        self.next(self.process, foreach="items")

    @step
    def process(self):
        self.processed = self.input * 2
        self.next(self.transform)

    @step
    def transform(self):
        self.result = self.processed + 1
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = sorted([i.result for i in inputs])
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    MultibodyForeachFlow()
