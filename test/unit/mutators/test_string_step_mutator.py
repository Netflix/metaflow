"""Tests for string-based StepMutator addition via MutableStep.add_decorator."""


def test_string_step_mutator_flow_completes(string_step_mutator_run):
    """Test that the flow completes successfully when adding a StepMutator by string."""
    assert string_step_mutator_run.successful


def test_string_step_mutator_ran(string_step_mutator_run):
    """StepMutator added by string should have its mutate() called,
    adding the environment variable."""
    task = string_step_mutator_run["start"].task
    assert task.data.step_mutator_val == "from_string"
