"""
Regression test for duplicate DAGTask names in foreach + split-switch flows.

When a split-switch's matching_conditional_join resolves to the same node as a
foreach's matching_join, the _visit function in _dag_templates could emit two
DAGTasks with the same name. Argo rejects this at submit time with:

    templates.<flow> sorting failed: duplicated nodeName <step>

This test verifies that the seen-tracking fix prevents the duplicate.
"""

import types

import pytest

from metaflow.plugins.argo.argo_workflows import ArgoWorkflows


class MockGraph:
    """Minimal graph mock that supports iteration and key access."""

    def __init__(self, nodes, start_step, end_step, name):
        self._nodes = nodes
        self.start_step = start_step
        self.end_step = end_step
        self.name = name

    def __getitem__(self, key):
        return self._nodes[key]

    def __iter__(self):
        return iter(self._nodes.values())


def _make_node(name, node_type="linear", out_funcs=None, in_funcs=None,
               is_inside_foreach=False, matching_join=None, foreach_param=None,
               parallel_foreach=False, parallel_step=False, split_parents=None,
               split_branches=None, decorators=None):
    """Create a minimal graph node stand-in."""
    return types.SimpleNamespace(
        name=name,
        type=node_type,
        out_funcs=out_funcs or [],
        in_funcs=in_funcs or [],
        is_inside_foreach=is_inside_foreach,
        matching_join=matching_join,
        foreach_param=foreach_param,
        parallel_foreach=parallel_foreach,
        parallel_step=parallel_step,
        split_parents=split_parents or [],
        split_branches=split_branches or [],
        decorators=decorators or [],
        conditional_branches=[],
    )


def _build_repro_graph():
    """
    Build the minimal graph that triggers the duplicate DAGTask bug.

    Flow structure:
        start (split-switch) -> optional_step, fan_gate
        optional_step -> fan_gate
        fan_gate (split-switch) -> fan_out, end
        fan_out (foreach) -> fan_step
        fan_step -> join_step (join)
        join_step -> end

    The bug: start's matching_conditional_join resolves to join_step,
    which is also fan_out's matching_join. This causes join_step to be
    emitted twice as a DAGTask.
    """
    nodes = {
        "start": _make_node(
            "start", node_type="split-switch",
            out_funcs=["optional_step", "fan_gate"],
        ),
        "optional_step": _make_node(
            "optional_step", node_type="linear",
            in_funcs=["start"],
            out_funcs=["fan_gate"],
        ),
        "fan_gate": _make_node(
            "fan_gate", node_type="split-switch",
            in_funcs=["start", "optional_step"],
            out_funcs=["fan_out", "end"],
        ),
        "fan_out": _make_node(
            "fan_out", node_type="foreach",
            in_funcs=["fan_gate"],
            out_funcs=["fan_step"],
            matching_join="join_step",
            foreach_param="scan_items",
        ),
        "fan_step": _make_node(
            "fan_step", node_type="linear",
            in_funcs=["fan_out"],
            out_funcs=["join_step"],
            is_inside_foreach=True,
        ),
        "join_step": _make_node(
            "join_step", node_type="join",
            in_funcs=["fan_step"],
            out_funcs=["end"],
            split_parents=["fan_out"],
        ),
        "end": _make_node(
            "end", node_type="end",
            in_funcs=["fan_gate", "join_step"],
        ),
    }
    return nodes


def test_no_duplicate_dag_task_names():
    """
    Verify that _dag_templates does not emit duplicate DAGTask names
    when a foreach matching_join coincides with a split-switch
    matching_conditional_join.
    """
    nodes = _build_repro_graph()

    instance = object.__new__(ArgoWorkflows)
    instance.graph = MockGraph(
        nodes, start_step="start", end_step="end",
        name="ForeachJoinDedupFlow",
    )

    flow = types.SimpleNamespace()
    flow.name = "ForeachJoinDedupFlow"
    instance.flow = flow

    # _dag_templates calls _daemon_templates which needs this
    instance.enable_heartbeat_daemon = False

    # Populate conditional tracking state that _dag_templates depends on
    instance._parse_conditional_branches()

    templates = instance._dag_templates()

    for template in templates:
        template_json = template.to_json()
        dag = template_json.get("dag")
        if not dag:
            continue

        task_names = [task["name"] for task in dag.get("tasks", [])]
        duplicates = [
            name for name in set(task_names)
            if task_names.count(name) > 1
        ]

        assert not duplicates, (
            f"Template '{template_json['name']}' has duplicate DAG task names: "
            f"{duplicates}"
        )