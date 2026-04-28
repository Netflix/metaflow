"""parent_tasks / child_tasks should match the DAG topology at runtime."""

from metaflow import FlowSpec, current, step


class RuntimeDagFlow(FlowSpec):
    @step
    def start(self):
        self.task_pathspec = current.pathspec
        self.next(self.middle)

    @step
    def middle(self):
        self.parent_pathspec = self.task_pathspec
        self.task_pathspec = current.pathspec
        self.next(self.end)

    @step
    def end(self):
        self.parent_pathspec = self.task_pathspec
        self.task_pathspec = current.pathspec


def test_runtime_dag(metaflow_runner, executor):
    result = metaflow_runner(RuntimeDagFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()

    # Child of start should be middle's task.
    start_task = run["start"].task
    middle_task = run["middle"].task
    end_task = run["end"].task

    parent_pathspecs = {t.pathspec for t in middle_task.parent_tasks}
    assert start_task.pathspec in parent_pathspecs

    parent_pathspecs = {t.pathspec for t in end_task.parent_tasks}
    assert middle_task.pathspec in parent_pathspecs

    child_pathspecs = {t.pathspec for t in start_task.child_tasks}
    assert middle_task.pathspec in child_pathspecs
