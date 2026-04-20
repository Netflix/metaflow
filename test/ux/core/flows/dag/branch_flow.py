import os

from metaflow import FlowSpec, step, project


@project(name="branch_flow")
class BranchFlow(FlowSpec):
    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.next(self.branch_a, self.branch_b)

    @step
    def branch_a(self):
        self.value = "a"
        self.next(self.join)

    @step
    def branch_b(self):
        self.value = "b"
        self.next(self.join)

    @step
    def join(self, inputs):
        self.values = sorted([i.value for i in inputs])
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    BranchFlow()
