"""Regression test for post_step returning (None, False) being a no-op."""


def test_post_step_none_false_completes(post_step_none_false_run):
    """Run completes successfully rather than hitting RuntimeError at
    task.py's `Invalid value passed to self.next` branch."""
    assert post_step_none_false_run.successful


def test_post_step_none_false_pre_step_ran(post_step_none_false_run):
    """Verify that the pre_step was executed on the start task."""
    task = post_step_none_false_run["start"].task
    assert task.data.pre_step_ran is True


def test_post_step_none_false_post_step_ran(post_step_none_false_run):
    """post_step ran and its (None, False) return value was accepted as
    a no-op (visible in the end step via data propagation)."""
    task = post_step_none_false_run["end"].task
    assert task.data.post_step_ran is True
