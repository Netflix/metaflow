"""Resume preserves origin-run-id / origin-task-id metadata correctly."""

from metaflow import FlowSpec, step
from metaflow_test import ResumeFromHere, is_resumed


class ResumeOriginpathFlow(FlowSpec):
    @step
    def start(self):
        self.data = "start"
        self.next(self.middle)

    @step
    def middle(self):
        self.data = "middle"
        if not is_resumed():
            raise ResumeFromHere()
        self.next(self.end)

    @step
    def end(self):
        pass


def test_resume_originpath(metaflow_runner, executor):
    first = metaflow_runner(ResumeOriginpathFlow, executor=executor)
    assert not first.successful
    assert first.run_id is not None  # start step should have written metadata

    resumed = metaflow_runner(
        ResumeOriginpathFlow, executor=executor, resume=True, resume_step="middle"
    )
    assert resumed.successful, resumed.stderr

    resumed_run = resumed.run()
    # Cloned start task: should reference the original run.
    start_task = resumed_run["start"].task
    md = start_task.metadata_dict
    assert md.get("origin-run-id") == first.run_id, (
        f"start expected origin-run-id={first.run_id}, got {md.get('origin-run-id')}"
    )
    assert "origin-task-id" in md, "cloned task should retain origin-task-id"

    # The re-run middle task: SHOULD have origin-run-id but NOT origin-task-id
    # (it's a fresh task that just shares the origin run's namespace).
    middle_task = resumed_run["middle"].task
    middle_md = middle_task.metadata_dict
    assert middle_md.get("origin-run-id") == first.run_id
    assert "origin-task-id" not in middle_md, (
        "re-run task should not carry origin-task-id"
    )
