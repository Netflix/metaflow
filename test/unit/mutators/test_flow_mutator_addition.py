"""Tests for dynamically adding FlowMutators via MutableFlow.add_decorator."""

# --- Adding a FlowMutator by class reference ---


def test_dynamic_flow_mutator_completes(dynamic_flow_mutator_run):
    """Test that the flow completes successfully when adding a FlowMutator by class reference."""
    assert dynamic_flow_mutator_run.successful


def test_dynamic_flow_mutator_inner_pre_mutate_ran(dynamic_flow_mutator_run):
    """Test InnerMutator.pre_mutate is called by the ongoing iteration."""
    task = dynamic_flow_mutator_run["start"].task
    assert task.data.inner_pre == "inner_pre_mutate_ran"


def test_dynamic_flow_mutator_inner_mutate_ran(dynamic_flow_mutator_run):
    """Test InnerMutator.mutate is also called by the ongoing iteration."""
    task = dynamic_flow_mutator_run["start"].task
    assert task.data.inner_mutate == "inner_mutate_ran"


# --- Adding a FlowMutator by string name with arguments ---


def test_string_flow_mutator_completes(string_flow_mutator_run):
    """Test that the flow completes successfully when adding a FlowMutator by string name."""
    assert string_flow_mutator_run.successful


def test_string_flow_mutator_pre_mutate_ran(string_flow_mutator_run):
    """Test StringAddedMutator.pre_mutate runs with the parsed arg."""
    task = string_flow_mutator_run["start"].task
    assert task.data.string_tag == "from_string"


def test_string_flow_mutator_mutate_ran(string_flow_mutator_run):
    """Test StringAddedMutator.mutate is also called."""
    task = string_flow_mutator_run["start"].task
    assert task.data.string_mutate == "yes"
