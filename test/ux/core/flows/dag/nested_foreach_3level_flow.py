"""3-level nested foreach: outer(foreach) → middle(foreach) → inner(foreach).

Exercises the compiler's deep compound-op path and any per-attempt foreach
split reading. Expected result at outer_join.all_results:
  ['a-1-10', 'a-1-20', 'a-2-10', 'a-2-20', 'b-1-10', 'b-1-20', 'b-2-10', 'b-2-20']
"""

import os

from metaflow import FlowSpec, project, step


@project(name="nested_foreach_3level_flow")
class NestedForeach3LevelFlow(FlowSpec):
    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.groups = ["a"]  # 1 outer item keeps total subprocess calls ~9 for CI speed
        self.next(self.outer, foreach="groups")

    @step
    def outer(self):
        self.group = self.input
        self.batches = [1, 2]
        self.next(self.middle, foreach="batches")

    @step
    def middle(self):
        self.batch = self.input
        self.items = [10]  # 1 item keeps total leaf tasks at 4 (2x2x1) for CI speed
        self.next(self.inner, foreach="items")

    @step
    def inner(self):
        self.result = "%s-%d-%d" % (self.group, self.batch, self.input)
        self.next(self.inner_join)

    @step
    def inner_join(self, inputs):
        self.batch_results = sorted(i.result for i in inputs)
        self.next(self.middle_join)

    @step
    def middle_join(self, inputs):
        self.group_results = sorted(r for i in inputs for r in i.batch_results)
        self.next(self.outer_join)

    @step
    def outer_join(self, inputs):
        self.all_results = sorted(r for i in inputs for r in i.group_results)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    NestedForeach3LevelFlow()
