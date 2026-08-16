"""
Integration tests for structural inference of start/end steps.

These tests actually execute flows with non-standard step names and verify:
- Flows run to completion
- _graph_info contains start_step/end_step
- _parameters metadata contains start_step/end_step
- Client APIs (end_task, parent_steps, child_steps) work correctly
- Single-step flows execute end-to-end
"""

import pytest
from metaflow.events import Trigger


# ---------------------------------------------------------------------------
# Shared Shape Tests (Parametrized)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "run_fixture",
    [
        "custom_named_run",
        "single_step_run",
        "single_step_bare_run",
        "custom_branch_run",
        "single_step_with_config_run",
        "single_step_with_stacked_decos_run",
        "single_step_with_flow_mutator_run",
    ],
    ids=[
        "custom_named",
        "single_step",
        "single_step_bare",
        "branch",
        "config",
        "stacked_decos",
        "flow_mutator",
    ],
)
def test_flow_completes_successfully(run_fixture, request):
    """Verify that various flow configurations execute end-to-end."""
    run = request.getfixturevalue(run_fixture)
    assert run.successful
    assert run.finished


@pytest.mark.parametrize(
    "run_fixture, expected_start, expected_end",
    [
        ("custom_named_run", "begin", "finish"),
        ("single_step_run", "only", "only"),
        ("single_step_bare_run", "only", "only"),
        ("custom_branch_run", "entry", "done"),
    ],
    ids=["custom_named", "single_step", "single_step_bare", "branch"],
)
def test_graph_info_endpoints(run_fixture, expected_start, expected_end, request):
    """Verify _graph_info captures the correct explicit start/end steps."""
    run = request.getfixturevalue(run_fixture)
    graph_info = run["_parameters"].task["_graph_info"].data
    assert graph_info["start_step"] == expected_start
    assert graph_info["end_step"] == expected_end


@pytest.mark.parametrize(
    "run_fixture, expected_start, expected_end",
    [
        ("custom_named_run", "begin", "finish"),
        ("single_step_run", "only", "only"),
        ("single_step_bare_run", "only", "only"),
    ],
    ids=["custom_named", "single_step", "single_step_bare"],
)
def test_parameters_metadata_endpoints(
    run_fixture, expected_start, expected_end, request
):
    """Verify metadata reflects the start/end step parameters correctly."""
    run = request.getfixturevalue(run_fixture)
    meta = run["_parameters"].task.metadata_dict
    assert meta.get("start_step") == expected_start
    assert meta.get("end_step") == expected_end


@pytest.mark.parametrize(
    "run_fixture, expected_val",
    [
        ("custom_named_run", 3),
        ("single_step_run", 42),
        ("single_step_bare_run", 42),
    ],
    ids=["custom_named", "single_step", "single_step_bare"],
)
def test_end_task_data_value(run_fixture, expected_val, request):
    """Verify the primary test artifact is correctly set in the terminal task."""
    run = request.getfixturevalue(run_fixture)
    assert run.end_task is not None
    assert run.end_task["x"].data == expected_val


@pytest.mark.parametrize(
    "run_fixture, expected_steps",
    [
        ("custom_named_run", {"begin", "middle", "finish"}),
        ("single_step_run", {"only"}),
        ("single_step_bare_run", {"only"}),
        ("custom_branch_run", {"entry", "a", "b", "merge", "done"}),
    ],
    ids=["custom_named", "single_step", "single_step_bare", "branch"],
)
def test_steps_present(run_fixture, expected_steps, request):
    """Verify the client API surfaces all executed step IDs correctly."""
    run = request.getfixturevalue(run_fixture)
    assert {step.id for step in run} == expected_steps


@pytest.mark.parametrize(
    "run_fixture",
    ["single_step_run", "single_step_bare_run"],
    ids=["single_step", "single_step_bare"],
)
def test_single_step_parent_child_empty(run_fixture, request):
    """Single-step flows should have no parent or child relationships."""
    run = request.getfixturevalue(run_fixture)
    assert list(run["only"].parent_steps) == []
    assert list(run["only"].child_steps) == []


# ---------------------------------------------------------------------------
# Flow-Specific Edge Cases & Composition
# ---------------------------------------------------------------------------


def test_custom_named_graph_endpoints_property(custom_named_run):
    start, end = custom_named_run._graph_endpoints
    assert start == "begin"
    assert end == "finish"


def test_custom_named_parent_steps(custom_named_run):
    assert list(custom_named_run["begin"].parent_steps) == []
    assert [step.id for step in custom_named_run["middle"].parent_steps] == ["begin"]
    assert [step.id for step in custom_named_run["finish"].parent_steps] == ["middle"]


def test_custom_named_child_steps(custom_named_run):
    assert [step.id for step in custom_named_run["begin"].child_steps] == ["middle"]
    assert [step.id for step in custom_named_run["middle"].child_steps] == ["finish"]
    assert list(custom_named_run["finish"].child_steps) == []


def test_branch_end_task_exists(custom_branch_run):
    assert custom_branch_run.end_task is not None


def test_branch_merge_data(custom_branch_run):
    merge_task = custom_branch_run["merge"].task
    assert sorted(merge_task["vals"].data) == ["a", "b"]


def test_branch_entry_has_two_children(custom_branch_run):
    children = [step.id for step in custom_branch_run["entry"].child_steps]
    assert sorted(children) == ["a", "b"]


def test_branch_merge_has_two_parents(custom_branch_run):
    parents = [step.id for step in custom_branch_run["merge"].parent_steps]
    assert sorted(parents) == ["a", "b"]


# ---------------------------------------------------------------------------
# Trigger Integration
# ---------------------------------------------------------------------------


def test_trigger_from_runs_uses_custom_terminal_step(custom_named_run):
    trigger = Trigger.from_runs([custom_named_run])

    assert trigger.event is not None
    assert trigger.event.name == "metaflow.%s.finish" % custom_named_run.parent.id
    assert trigger.event.id == custom_named_run.end_task.pathspec
    assert trigger.run.pathspec == custom_named_run.pathspec


# ---------------------------------------------------------------------------
# Composition Specific Tests
# ---------------------------------------------------------------------------


def test_single_step_with_config_value_flows_to_artifact(single_step_with_config_run):
    """Config descriptor value is readable from the end task's artifact."""
    end_task = single_step_with_config_run.end_task
    assert end_task["v"].data == 7


def test_single_step_with_stacked_decos_graph_info(single_step_with_stacked_decos_run):
    """_graph_info records all stacked decorators on the only step."""
    graph_info = (
        single_step_with_stacked_decos_run["_parameters"].task["_graph_info"].data
    )
    names = {deco["name"] for deco in graph_info["steps"]["only"]["decorators"]}
    assert {"retry", "resources"}.issubset(names)


def test_single_step_with_flow_mutator_applied(single_step_with_flow_mutator_run):
    """FlowMutator.add_decorator landed @retry on the only step."""
    graph_info = (
        single_step_with_flow_mutator_run["_parameters"].task["_graph_info"].data
    )
    names = {deco["name"] for deco in graph_info["steps"]["only"]["decorators"]}
    assert "retry" in names
