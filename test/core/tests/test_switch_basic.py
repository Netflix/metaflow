"""self.next(switch={...}, condition='attr') routes to one of N branches."""

from metaflow import FlowSpec, step


class SwitchBasicFlow(FlowSpec):
    @step
    def start(self):
        self.choice = "left"
        self.next({"left": self.left, "right": self.right}, condition="choice")

    @step
    def left(self):
        self.taken = "left"
        self.next(self.end)

    @step
    def right(self):
        self.taken = "right"
        self.next(self.end)

    @step
    def end(self):
        pass


def test_switch_basic(metaflow_runner, executor):
    result = metaflow_runner(SwitchBasicFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    # With choice="left", left ran; right did not (no Step entry).
    step_names = {step.id for step in run}
    assert "left" in step_names
    assert "right" not in step_names
    assert run["left"].task.data.taken == "left"
