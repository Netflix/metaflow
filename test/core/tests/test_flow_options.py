"""Top-level flow options like --quiet / --datastore propagate correctly."""

from metaflow import FlowSpec, step


class FlowOptionsFlow(FlowSpec):
    @step
    def start(self):
        import os

        self.metaflow_user = os.environ.get("METAFLOW_USER")
        self.next(self.end)

    @step
    def end(self):
        pass


def test_flow_options_metaflow_user(metaflow_runner, executor):
    result = metaflow_runner(FlowOptionsFlow, executor=executor)
    assert result.successful, result.stderr
    # METAFLOW_USER set by tox/conftest is visible inside the step subprocess.
    assert result.run()["start"].task.data.metaflow_user == "tester"
