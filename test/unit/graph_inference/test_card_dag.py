"""
Tests for the cards' DAG-related graph data layer.

Covers:
- `transform_flow_graph` - the function that normalizes the legacy
  flat-step-dict and the new `{steps, start_step, end_step}` shapes
  into the structure the DAG card component renders against.
- The end-to-end render path: a Run with custom-named entry/terminal
  steps produces a DAG card whose `start_step` / `end_step` /
  `steps` reflect the user's actual step names rather than the
  legacy hardcoded "start" / "end".
"""

import json

from metaflow.plugins.cards.card_modules.basic import (
    DefaultCardJSON,
    transform_flow_graph,
)


def _find_components_by_type(node, component_type):
    if isinstance(node, dict):
        if node.get("type") == component_type:
            yield node
        for value in node.values():
            yield from _find_components_by_type(value, component_type)
    elif isinstance(node, list):
        for item in node:
            yield from _find_components_by_type(item, component_type)


# ---------------------------------------------------------------------------
# transform_flow_graph: shape-detection unit tests
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
# DefaultCardJSON: end-to-end render with custom-named endpoints
# ---------------------------------------------------------------------------


def test_default_card_includes_custom_graph_endpoints(
    custom_named_card_run, custom_named_card_flow
):
    graph = custom_named_card_run["_parameters"].task["_graph_info"].data
    card_data = json.loads(
        DefaultCardJSON(graph=graph, flow=custom_named_card_flow).render(
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
