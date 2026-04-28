"""metaflow client artifact lineage should match the flow's actual data dependencies."""

from metaflow import FlowSpec, current, step


class LineageFlow(FlowSpec):
    """A diamond flow so lineage isn't trivially identical to artifact propagation."""

    @step
    def start(self):
        # capture this step's pathspec so the test can verify ancestry.
        self.pathspec_at_start = current.pathspec
        self.x = 1
        self.next(self.left, self.right)

    @step
    def left(self):
        self.pathspec_at_left = current.pathspec
        self.left_data = self.x + 10
        self.next(self.join)

    @step
    def right(self):
        self.pathspec_at_right = current.pathspec
        self.right_data = self.x + 100
        self.next(self.join)

    @step
    def join(self, inputs):
        self.merge_artifacts(inputs)
        self.pathspec_at_join = current.pathspec
        self.next(self.end)

    @step
    def end(self):
        self.pathspec_at_end = current.pathspec


def test_lineage(metaflow_runner, executor):
    result = metaflow_runner(LineageFlow, executor=executor)
    assert result.successful, result.stderr
    run = result.run()

    # Artifact propagation through the diamond.
    assert run["start"].task.data.x == 1
    assert run["left"].task.data.left_data == 11
    assert run["right"].task.data.right_data == 101

    # Lineage: end's parent is join; join's parents are left+right; left
    # and right both have start as their parent.
    end_task = run["end"].task
    join_task = run["join"].task
    left_task = run["left"].task
    right_task = run["right"].task
    start_task = run["start"].task

    assert {p.pathspec for p in end_task.parent_tasks} == {join_task.pathspec}
    assert {p.pathspec for p in join_task.parent_tasks} == {
        left_task.pathspec,
        right_task.pathspec,
    }
    assert {p.pathspec for p in left_task.parent_tasks} == {start_task.pathspec}
    assert {p.pathspec for p in right_task.parent_tasks} == {start_task.pathspec}

    # Children: start has both branches as children; join has end as child.
    assert {c.pathspec for c in start_task.child_tasks} == {
        left_task.pathspec,
        right_task.pathspec,
    }
    assert {c.pathspec for c in join_task.child_tasks} == {end_task.pathspec}
