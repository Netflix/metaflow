"""Flow with a nested split inside a branch.

Graph:
  start → [branch_a, branch_b] → outer_join → end
  branch_a → [inner_x, inner_y] → inner_join → outer_join

Used to verify that schedulers correctly compile and execute complex graph
topologies where a split exists inside one branch of an outer split.
Without correct join detection, outer_join and end are silently dropped.
"""

from metaflow import FlowSpec, step


class SplitInBranchFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.branch_a, self.branch_b)

    @step
    def branch_a(self):
        self.label = "a"
        self.next(self.inner_x, self.inner_y)

    @step
    def inner_x(self):
        self.sub_label = "x"
        # In subprocess execution each step only persists explicitly-assigned
        # attributes. Re-assign inherited 'label' so it survives to inner_join.
        self.label = self.label
        self.next(self.inner_join)

    @step
    def inner_y(self):
        self.sub_label = "y"
        self.label = self.label
        self.next(self.inner_join)

    @step
    def inner_join(self, inputs):
        self.sub_labels = sorted(i.sub_label for i in inputs)
        # Propagate label through the join for outer_join to read.
        self.label = inputs[0].label
        self.next(self.outer_join)

    @step
    def branch_b(self):
        self.label = "b"
        self.next(self.outer_join)

    @step
    def outer_join(self, inputs):
        self.labels = sorted(i.label for i in inputs)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    SplitInBranchFlow()
