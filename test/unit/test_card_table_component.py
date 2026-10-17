"""Regression tests for non-finite floats breaking Table card rendering (#1023)."""

import json

import pytest

from metaflow.plugins.cards.card_modules.basic import TableComponent


def test_table_component_replaces_nan_with_none():
    component = TableComponent(headers=["a", "b"], data=[[1, 2], [3, float("nan")]])

    rendered = component.render()

    assert rendered["data"] == [[1, 2], [3, None]]


@pytest.mark.parametrize(
    "value", [float("nan"), float("inf"), float("-inf")], ids=["nan", "inf", "-inf"]
)
def test_table_component_render_is_strict_json_serializable(value):
    # json.dumps allows NaN/Infinity/-Infinity by default, emitting the bare
    # (non-standard) tokens `NaN`/`Infinity`/`-Infinity`. The card frontend's
    # `JSON.parse` rejects those tokens, so `allow_nan=False` is what actually
    # exercises the real failure mode: it raises unless every float is finite.
    component = TableComponent(headers=["x"], data=[[1], [value]])

    rendered = component.render()

    json.dumps(rendered, allow_nan=False)


def test_table_component_preserves_finite_values_and_non_float_types():
    data = [[1, "text", 2.5], [None, True, -3]]
    component = TableComponent(headers=["a", "b", "c"], data=data)

    rendered = component.render()

    assert rendered["data"] == data
