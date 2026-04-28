"""A switch whose branches are themselves switches."""

from metaflow import FlowSpec, step


class SwitchNestedFlow(FlowSpec):
    @step
    def start(self):
        self.outer = "A"
        self.next({"A": self.outer_a, "B": self.outer_b}, condition="outer")

    @step
    def outer_a(self):
        self.inner = "x"
        self.next({"x": self.leaf_x, "y": self.leaf_y}, condition="inner")

    @step
    def outer_b(self):
        self.next(self.leaf_x)

    @step
    def leaf_x(self):
        self.taken = "x"
        self.next(self.end)

    @step
    def leaf_y(self):
        self.taken = "y"
        self.next(self.end)

    @step
    def end(self):
        pass


def test_switch_nested(metaflow_runner, executor):
    result = metaflow_runner(SwitchNestedFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    step_names = {step.id for step in run}
    # outer="A", inner="x" → outer_a + leaf_x ran; outer_b + leaf_y did not.
    assert "outer_a" in step_names
    assert "leaf_x" in step_names
    assert "outer_b" not in step_names
    assert "leaf_y" not in step_names
    assert run["leaf_x"].task.data.taken == "x"
