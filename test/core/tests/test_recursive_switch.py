"""A switch that re-enters itself based on iteration count."""

from metaflow import FlowSpec, step


class RecursiveSwitchFlow(FlowSpec):
    @step
    def start(self):
        self.iteration = 0
        self.choice = "again"
        self.next({"again": self.body, "stop": self.end}, condition="choice")

    @step
    def body(self):
        self.iteration += 1
        if self.iteration >= 3:
            self.choice = "stop"
        else:
            self.choice = "again"
        self.next({"again": self.body, "stop": self.end}, condition="choice")

    @step
    def end(self):
        pass


def test_recursive_switch(metaflow_runner, executor):
    result = metaflow_runner(RecursiveSwitchFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()
    # body should have been executed 3 times (iterations 1, 2, 3).
    body_tasks = list(run["body"])
    assert len(body_tasks) == 3
    iterations = sorted(t.data.iteration for t in body_tasks)
    assert iterations == [1, 2, 3]
