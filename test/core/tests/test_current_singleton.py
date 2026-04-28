"""metaflow.current exposes the right values at each phase of execution."""

from metaflow import FlowSpec, current, project, step


@project(name="current_singleton")
class CurrentSingletonFlow(FlowSpec):
    @step
    def start(self):
        self.observed = {
            "flow_name": current.flow_name,
            "step_name": current.step_name,
            "project_name": current.project_name,
            "branch_name": current.branch_name,
            "is_production": current.is_production,
            "username": current.username,
            "namespace": current.namespace,
            "run_id_in_start": current.run_id,
        }
        self.next(self.end)

    @step
    def end(self):
        # current.run_id is stable across steps.
        assert current.run_id == self.observed["run_id_in_start"]


def test_current_singleton(metaflow_runner, executor):
    result = metaflow_runner(CurrentSingletonFlow, executor=executor)
    assert result.successful, result.stderr
    obs = result.run()["start"].task.data.observed
    assert obs["flow_name"] == "CurrentSingletonFlow"
    assert obs["step_name"] == "start"
    assert obs["project_name"] == "current_singleton"
    # project_name is set, so branch_name is "user.<user>" by default.
    assert obs["branch_name"] is not None
    assert obs["is_production"] is False
    assert obs["username"]
