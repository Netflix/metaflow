"""A switch inside a branch (top-level branch, switch in one of the branches)."""

from metaflow import FlowSpec, step


class SwitchInBranchFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.left, self.right)

    @step
    def left(self):
        self.choice = "x"
        self.next({"x": self.left_x, "y": self.left_y}, condition="choice")

    @step
    def left_x(self):
        self.taken = "left_x"
        self.next(self.merge)

    @step
    def left_y(self):
        self.taken = "left_y"
        self.next(self.merge)

    @step
    def right(self):
        self.taken = "right"
        self.next(self.merge)

    @step
    def merge(self, inputs):
        self.merged = sorted(i.taken for i in inputs)
        self.next(self.end)

    @step
    def end(self):
        pass


def test_switch_in_branch(metaflow_runner, executor):
    result = metaflow_runner(SwitchInBranchFlow, executor=executor)
    assert result.successful, result.stderr
    merged = result.run()["merge"].task.data.merged
    assert merged == ["left_x", "right"]
