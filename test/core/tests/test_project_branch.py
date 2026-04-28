"""@project branch_name set via --branch flag at run-time."""

from metaflow import FlowSpec, current, project, step


@project(name="branch_test")
class ProjectBranchFlow(FlowSpec):
    @step
    def start(self):
        self.observed_branch = current.branch_name
        self.next(self.end)

    @step
    def end(self):
        pass


def test_project_branch_default(metaflow_runner, executor):
    result = metaflow_runner(ProjectBranchFlow, executor=executor)
    assert result.successful, result.stderr
    branch = result.run()["start"].task.data.observed_branch
    # Without --branch override, default is "user.<USER>".
    assert branch is not None
    assert branch.startswith("user.")
