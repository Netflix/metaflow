"""Tests that MutableStep.add_decorator returns the decorator instance."""

import pytest


@pytest.fixture
def start_task_data(add_decorator_return_run):
    """Shared setup: Extracts the data payload from the start task of the test run."""
    return add_decorator_return_run["start"].task.data


def test_add_decorator_flow_completes_successfully(add_decorator_return_run):
    """Test that the flow modified with add_decorator completes without errors."""
    assert add_decorator_return_run.successful


def test_add_decorator_returns_instance(start_task_data):
    """Test that add_decorator returns an actual object, not None."""
    assert start_task_data.returned_is_none is False


def test_returned_decorator_instance_has_name_attribute(start_task_data):
    """Test that the returned decorator instance has the expected properties."""
    assert start_task_data.returned_has_name is True


def test_added_decorator_executes_and_sets_data(start_task_data):
    """Test that the dynamically added decorator was actually executed during the run."""
    # Verify that the variable injected by the dynamically added decorator exists
    assert start_task_data.added_var == "from_mutator"


def test_adding_duplicate_decorator_with_ignore_returns_none(start_task_data):
    """Test that attempting to add a duplicate decorator (when ignored) returns None."""
    assert start_task_data.duplicate_is_none is True


def test_ignored_duplicate_decorator_does_not_execute(start_task_data):
    """Test that the ignored duplicate decorator does not apply its logic."""
    assert start_task_data.should_not_exist is None
