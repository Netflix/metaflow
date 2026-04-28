"""A foreach inside a foreach: outer × inner = full grid of tasks."""

from metaflow import FlowSpec, step


class NestedForeachFlow(FlowSpec):
    @step
    def start(self):
        self.outer_arr = ["a", "b"]
        self.next(self.outer, foreach="outer_arr")

    @step
    def outer(self):
        self.outer_value = self.input
        self.inner_arr = [1, 2, 3]
        self.next(self.inner, foreach="inner_arr")

    @step
    def inner(self):
        self.cell = (self.outer_value, self.input)
        self.next(self.inner_join)

    @step
    def inner_join(self, inputs):
        self.cells = [inp.cell for inp in inputs]
        self.next(self.outer_join)

    @step
    def outer_join(self, inputs):
        self.all_cells = sorted(c for inp in inputs for c in inp.cells)
        self.next(self.end)

    @step
    def end(self):
        pass


def test_nested_foreach(metaflow_runner, executor):
    result = metaflow_runner(NestedForeachFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    cells = run["end"].task.data.all_cells
    expected = sorted([("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2), ("b", 3)])
    assert cells == expected
