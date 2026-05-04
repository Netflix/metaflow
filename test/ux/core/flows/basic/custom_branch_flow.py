"""A branching flow with custom step names using start/end annotations."""

from metaflow import FlowSpec, step, project


@project(name="custom_branch")
class CustomBranchFlow(FlowSpec):
    @step(start=True)
    def entry(self):
        self.val = "root"
        self.next(self.left, self.right)

    @step
    def left(self):
        self.branch_val = "left"
        self.next(self.merge)

    @step
    def right(self):
        self.branch_val = "right"
        self.next(self.merge)

    @step
    def merge(self, inputs):
        self.branches = sorted([inp.branch_val for inp in inputs])
        self.next(self.done)

    @step(end=True)
    def done(self):
        self.result = self.branches


if __name__ == "__main__":
    CustomBranchFlow()
