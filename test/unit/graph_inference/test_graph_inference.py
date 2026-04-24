"""
Integration tests for structural inference of start/end steps.

These tests actually execute flows with non-standard step names and verify:
- Flows run to completion
- _graph_info contains start_step/end_step
- _parameters metadata contains start_step/end_step
- Client APIs (end_task, parent_steps, child_steps) work correctly
- Single-step flows execute end-to-end
"""

import json
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from metaflow.events import Trigger
from metaflow.plugins.cards.card_modules.basic import (
    DefaultCardJSON,
    transform_flow_graph,
)

FLOWS_DIR = Path(__file__).resolve().parent / "flows"


def _find_components_by_type(node, component_type):
    if isinstance(node, dict):
        if node.get("type") == component_type:
            yield node
        for value in node.values():
            yield from _find_components_by_type(value, component_type)
    elif isinstance(node, list):
        for item in node:
            yield from _find_components_by_type(item, component_type)


def _load_flow(flow_file, flow_class_name):
    spec = spec_from_file_location(flow_class_name, FLOWS_DIR / flow_file)
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return getattr(module, flow_class_name)(use_cli=False)


# ---------------------------------------------------------------------------
# Custom named flow (begin/middle/finish)
# ---------------------------------------------------------------------------


def test_custom_named_flow_completes(custom_named_run):
    assert custom_named_run.successful
    assert custom_named_run.finished


def test_custom_named_graph_info_has_endpoints(custom_named_run):
    graph_info = custom_named_run["_parameters"].task["_graph_info"].data
    assert graph_info["start_step"] == "begin"
    assert graph_info["end_step"] == "finish"


def test_custom_named_parameters_metadata_has_endpoints(custom_named_run):
    meta = custom_named_run["_parameters"].task.metadata_dict
    assert meta.get("start_step") == "begin"
    assert meta.get("end_step") == "finish"


def test_custom_named_graph_endpoints_property(custom_named_run):
    start, end = custom_named_run._graph_endpoints
    assert start == "begin"
    assert end == "finish"


def test_custom_named_end_task(custom_named_run):
    end_task = custom_named_run.end_task
    assert end_task is not None
    assert end_task["x"].data == 3


def test_custom_named_steps_present(custom_named_run):
    step_names = {s.id for s in custom_named_run}
    assert step_names == {"begin", "middle", "finish"}


def test_custom_named_parent_steps(custom_named_run):
    assert list(custom_named_run["begin"].parent_steps) == []
    assert [s.id for s in custom_named_run["middle"].parent_steps] == ["begin"]
    assert [s.id for s in custom_named_run["finish"].parent_steps] == ["middle"]


def test_custom_named_child_steps(custom_named_run):
    assert [s.id for s in custom_named_run["begin"].child_steps] == ["middle"]
    assert [s.id for s in custom_named_run["middle"].child_steps] == ["finish"]
    assert list(custom_named_run["finish"].child_steps) == []


# ---------------------------------------------------------------------------
# Single-step flow (start == end)
# ---------------------------------------------------------------------------


def test_single_step_flow_completes(single_step_run):
    assert single_step_run.successful
    assert single_step_run.finished


def test_single_step_graph_info_start_equals_end(single_step_run):
    graph_info = single_step_run["_parameters"].task["_graph_info"].data
    assert graph_info["start_step"] == "only"
    assert graph_info["end_step"] == "only"
    assert graph_info["start_step"] == graph_info["end_step"]


def test_single_step_parameters_metadata(single_step_run):
    meta = single_step_run["_parameters"].task.metadata_dict
    assert meta.get("start_step") == "only"
    assert meta.get("end_step") == "only"


def test_single_step_end_task(single_step_run):
    end_task = single_step_run.end_task
    assert end_task is not None
    assert end_task["x"].data == 42


def test_single_step_present(single_step_run):
    assert {s.id for s in single_step_run} == {"only"}


def test_single_step_parent_child_empty(single_step_run):
    assert list(single_step_run["only"].parent_steps) == []
    assert list(single_step_run["only"].child_steps) == []


# ---------------------------------------------------------------------------
# Custom branch flow (entry/a/b/merge/done)
# ---------------------------------------------------------------------------


def test_branch_flow_completes(custom_branch_run):
    assert custom_branch_run.successful
    assert custom_branch_run.finished


def test_branch_graph_info_endpoints(custom_branch_run):
    graph_info = custom_branch_run["_parameters"].task["_graph_info"].data
    assert graph_info["start_step"] == "entry"
    assert graph_info["end_step"] == "done"


def test_branch_end_task(custom_branch_run):
    assert custom_branch_run.end_task is not None


def test_branch_merge_data(custom_branch_run):
    merge_task = custom_branch_run["merge"].task
    assert sorted(merge_task["vals"].data) == ["a", "b"]


def test_branch_steps_present(custom_branch_run):
    assert {s.id for s in custom_branch_run} == {"entry", "a", "b", "merge", "done"}


def test_branch_entry_has_two_children(custom_branch_run):
    children = [s.id for s in custom_branch_run["entry"].child_steps]
    assert sorted(children) == ["a", "b"]


def test_branch_merge_has_two_parents(custom_branch_run):
    parents = [s.id for s in custom_branch_run["merge"].parent_steps]
    assert sorted(parents) == ["a", "b"]


# ---------------------------------------------------------------------------
# Trigger integration
# ---------------------------------------------------------------------------


def test_trigger_from_runs_uses_custom_terminal_step(custom_named_run):
    trigger = Trigger.from_runs([custom_named_run])

    assert trigger.event is not None
    assert trigger.event.name == "metaflow.%s.finish" % custom_named_run.parent.id
    assert trigger.event.id == custom_named_run.end_task.pathspec
    assert trigger.run.pathspec == custom_named_run.pathspec


# ---------------------------------------------------------------------------
# Card graph transform
# ---------------------------------------------------------------------------


def test_transform_flow_graph_supports_explicit_endpoints():
    graph = {
        "start_step": "begin",
        "end_step": "finish",
        "steps": {
            "begin": {"type": "start", "next": ["middle"], "doc": "begin"},
            "middle": {"type": "linear", "next": ["finish"], "doc": "middle"},
            "finish": {"type": "end", "next": [], "doc": "finish"},
        },
    }

    transformed = transform_flow_graph(graph)

    assert transformed["start_step"] == "begin"
    assert transformed["end_step"] == "finish"
    assert set(transformed["steps"]) == {"begin", "middle", "finish"}
    assert transformed["steps"]["begin"]["type"] == "start"
    assert transformed["steps"]["middle"]["box_next"] is False
    assert transformed["steps"]["finish"]["type"] == "end"


def test_transform_flow_graph_keeps_legacy_start_end_detection():
    graph = {
        "start": {"type": "start", "next": ["end"], "doc": ""},
        "end": {"type": "end", "next": [], "doc": ""},
    }

    transformed = transform_flow_graph(graph)

    assert transformed["start_step"] == "start"
    assert transformed["end_step"] == "end"
    assert set(transformed["steps"]) == {"start", "end"}


# ---------------------------------------------------------------------------
# Cards integration
# ---------------------------------------------------------------------------


def test_default_card_includes_custom_graph_endpoints(custom_named_card_run):
    flow = _load_flow("custom_named_card_flow.py", "CustomNamedCardFlow")
    graph = custom_named_card_run["_parameters"].task["_graph_info"].data
    card_data = json.loads(
        DefaultCardJSON(graph=graph, flow=flow).render(
            custom_named_card_run["begin"].task
        )
    )

    dag_components = list(_find_components_by_type(card_data["components"], "dag"))
    assert len(dag_components) == 1

    dag_data = dag_components[0]["data"]
    assert dag_data["start_step"] == "begin"
    assert dag_data["end_step"] == "finish"
    assert set(dag_data["steps"]) == {"begin", "middle", "finish"}
    assert "start" not in dag_data["steps"]
    assert "end" not in dag_data["steps"]


# ---------------------------------------------------------------------------
# Composition: single-step flows with Config, stacked decorators, FlowMutator
# ---------------------------------------------------------------------------


def test_single_step_with_config_completes(single_step_with_config_run):
    """Config-bearing single-step flow runs to completion."""
    assert single_step_with_config_run.successful
    assert single_step_with_config_run.finished


def test_single_step_with_config_value_flows_to_artifact(single_step_with_config_run):
    """Config descriptor value is readable from the end task's artifact."""
    end_task = single_step_with_config_run.end_task
    assert end_task["v"].data == 7


def test_single_step_with_stacked_decos_completes(single_step_with_stacked_decos_run):
    """Single-step flow with stacked @retry/@resources runs end-to-end."""
    assert single_step_with_stacked_decos_run.successful
    assert single_step_with_stacked_decos_run.finished


def test_single_step_with_stacked_decos_graph_info(single_step_with_stacked_decos_run):
    """_graph_info records all stacked decorators on the only step."""
    graph_info = (
        single_step_with_stacked_decos_run["_parameters"].task["_graph_info"].data
    )
    names = {d["name"] for d in graph_info["steps"]["only"]["decorators"]}
    assert {"retry", "resources"}.issubset(names)


def test_single_step_with_flow_mutator_completes(single_step_with_flow_mutator_run):
    """FlowMutator-decorated single-step flow runs end-to-end."""
    assert single_step_with_flow_mutator_run.successful
    assert single_step_with_flow_mutator_run.finished


def test_single_step_with_flow_mutator_applied(single_step_with_flow_mutator_run):
    """FlowMutator.add_decorator landed @retry on the only step."""
    graph_info = (
        single_step_with_flow_mutator_run["_parameters"].task["_graph_info"].data
    )
    names = {d["name"] for d in graph_info["steps"]["only"]["decorators"]}
    assert "retry" in names
