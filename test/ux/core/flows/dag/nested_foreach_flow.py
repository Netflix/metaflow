import os

from metaflow import FlowSpec, step, project


@project(name="nested_foreach_flow")
class NestedForeachFlow(FlowSpec):
    @step
    def start(self):
        self.execution_env = os.environ.get("KUBERNETES_SERVICE_HOST", "")
        self.groups = ["x", "y"]
        self.next(self.outer, foreach="groups")

    @step
    def outer(self):
        self.group = self.input
        self.items = [1]
        self.next(self.inner, foreach="items")

    @step
    def inner(self):
        self.result = "%s-%d" % (self.group, self.input)
        self.next(self.inner_join)

    @step
    def inner_join(self, inputs):
        self.inner_results = sorted([i.result for i in inputs])
        self.next(self.outer_join)

    @step
    def outer_join(self, inputs):
        self.all_results = sorted([r for i in inputs for r in i.inner_results])
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    NestedForeachFlow()
