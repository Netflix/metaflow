"""Tests for dynamically adding FlowMutators via MutableFlow.add_decorator."""


class TestDynamicFlowMutatorAddition:
    """Adding a FlowMutator by class reference."""

    def test_flow_completes(self, dynamic_flow_mutator_run):
        assert dynamic_flow_mutator_run.successful

    def test_inner_pre_mutate_ran(self, dynamic_flow_mutator_run):
        """InnerMutator.pre_mutate should have been called by the ongoing iteration."""
        task = dynamic_flow_mutator_run["start"].task
        assert task.data.inner_pre == "inner_pre_mutate_ran"

    def test_inner_mutate_ran(self, dynamic_flow_mutator_run):
        """InnerMutator.mutate should also have been called."""
        task = dynamic_flow_mutator_run["start"].task
        assert task.data.inner_mutate == "inner_mutate_ran"


class TestStringFlowMutatorAddition:
    """Adding a FlowMutator by string name with arguments."""

    def test_flow_completes(self, string_flow_mutator_run):
        assert string_flow_mutator_run.successful

    def test_string_mutator_pre_mutate_ran(self, string_flow_mutator_run):
        """StringAddedMutator.pre_mutate should have run with the parsed arg."""
        task = string_flow_mutator_run["start"].task
        assert task.data.string_tag == "from_string"

    def test_string_mutator_mutate_ran(self, string_flow_mutator_run):
        """StringAddedMutator.mutate should also have been called."""
        task = string_flow_mutator_run["start"].task
        assert task.data.string_mutate == "yes"
