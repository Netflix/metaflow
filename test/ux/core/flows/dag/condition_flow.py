import os

from metaflow import FlowSpec, condition, project, step


@project(name="condition_flow")
class ConditionFlow(FlowSpec):
    """Flow with a conditional branch (@condition).

    Exercises the split-switch topology: the start step sets a flag,
    the condition step routes to either high_branch or low_branch,
    and both branches converge at merge.
    """

    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.value = 42
        self.next(self.check)

    @condition
    @step
    def check(self):
        self.is_high = self.value >= 10
        self.next(self.high_branch, self.low_branch)

    @step
    def high_branch(self):
        self.branch = "high"
        self.result = self.value * 2
        self.next(self.merge)

    @step
    def low_branch(self):
        self.branch = "low"
        self.result = self.value + 1
        self.next(self.merge)

    @step
    def merge(self):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ConditionFlow()
