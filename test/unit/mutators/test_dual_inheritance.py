"""Tests for dual UserStepDecorator + StepMutator inheritance."""

import pytest


@pytest.fixture
def start_task_data(dual_inherit_run):
    """Shared setup: Extracts the data payload from the start task of the test run."""
    return dual_inherit_run["start"].task.data


@pytest.fixture
def end_task_data(dual_inherit_run):
    """Shared setup: Extracts the data payload from the end task of the test run."""
    return dual_inherit_run["end"].task.data


def test_dual_inheritance_flow_completes_successfully(dual_inherit_run):
    """Test that a flow using a decorator with dual inheritance runs to completion."""
    assert dual_inherit_run.successful


def test_pre_mutate_hook_adds_environment_variable(start_task_data):
    """Test that the pre_mutate() hook correctly injects its environment variable."""
    assert start_task_data.pre_mutate_env_var == "pre_mutate_ran"


def test_mutate_hook_adds_environment_variable(start_task_data):
    """Test that the mutate() hook correctly injects its environment variable."""
    assert start_task_data.mutate_env_var == "hello"


def test_pre_step_hook_sets_artifact(start_task_data):
    """Test that the pre_step() hook executes and successfully sets an artifact."""
    assert start_task_data.pre_step_ran is True


def test_post_step_hook_sets_artifact_visible_downstream(end_task_data):
    """
    Test that post_step() sets an artifact on the start step,
    and verifies it is visible in the end step via data propagation.
    """
    assert end_task_data.post_step_ran is True
