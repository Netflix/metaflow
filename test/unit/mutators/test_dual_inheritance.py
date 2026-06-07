"""Tests for dual UserStepDecorator + StepMutator inheritance."""


class TestDualInheritance:
    def test_flow_completes(self, dual_inherit_run):
        assert dual_inherit_run.successful

    def test_pre_mutate_ran(self, dual_inherit_run):
        """pre_mutate() should have added the environment variable."""
        task = dual_inherit_run["start"].task
        assert task.data.pre_mutate_env_var == "pre_mutate_ran"

    def test_mutate_ran(self, dual_inherit_run):
        """mutate() should have added the environment variable."""
        task = dual_inherit_run["start"].task
        assert task.data.mutate_env_var == "hello"

    def test_pre_step_ran(self, dual_inherit_run):
        """pre_step() should have set the artifact."""
        task = dual_inherit_run["start"].task
        assert task.data.pre_step_ran is True

    def test_post_step_ran(self, dual_inherit_run):
        """post_step() should have set the artifact on the start step,
        visible in the end step via data propagation."""
        task = dual_inherit_run["end"].task
        assert task.data.post_step_ran is True
