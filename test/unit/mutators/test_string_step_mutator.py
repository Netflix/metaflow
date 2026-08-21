"""Tests for string-based StepMutator addition via MutableStep.add_decorator."""


class TestStringStepMutatorAddition:
    def test_flow_completes(self, string_step_mutator_run):
        assert string_step_mutator_run.successful

    def test_string_step_mutator_ran(self, string_step_mutator_run):
        """StepMutator added by string should have its mutate() called,
        adding the environment variable."""
        task = string_step_mutator_run["start"].task
        assert task.data.step_mutator_val == "from_string"
