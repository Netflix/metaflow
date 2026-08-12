"""
Tests for nested conditional (split-switch) join dependency generation.

Reproduces https://github.com/Netflix/metaflow/issues/3334 — when an outer
split-switch sorts alphabetically before an inner split-switch, the merge step
depends were computed as && instead of ||.
"""

import pytest

from metaflow import FlowSpec, step
from metaflow.plugins.argo.argo_workflows import ArgoWorkflows


# ── Flows ────────────────────────────────────────────────────────────────────
# Outer sorts BEFORE inner alphabetically (a_ < b_).  This is the order that
# triggers the bug.


class NestedSwitchAlphaFlow(FlowSpec):
    """outer=a_outer_switch, inner=b_inner_switch → outer < inner"""

    @step
    def start(self):
        self.outer_route = "a_branch_a"
        self.next(
            {
                "a_branch_a": self.a_branch_a,
                "a_branch_b": self.a_branch_b,
            },
            condition="outer_route",
        )

    @step
    def a_branch_a(self):
        self.inner_route = "b_sub_a"
        self.next(
            {
                "b_sub_a": self.b_sub_a,
                "b_sub_b": self.b_sub_b,
            },
            condition="inner_route",
        )

    @step
    def b_sub_a(self):
        self.next(self.inner_join)

    @step
    def b_sub_b(self):
        self.next(self.inner_join)

    @step
    def inner_join(self):
        self.next(self.outer_join)

    @step
    def a_branch_b(self):
        self.next(self.outer_join)

    @step
    def outer_join(self):
        self.next(self.end)

    @step
    def end(self):
        pass


# Same topology, but inner sorts BEFORE outer (x_ < z_).
# This order has always worked.


class NestedSwitchReverseFlow(FlowSpec):
    """outer=z_outer_switch, inner=x_inner_switch → inner < outer"""

    @step
    def start(self):
        self.outer_route = "z_branch_a"
        self.next(
            {
                "z_branch_a": self.z_branch_a,
                "z_branch_b": self.z_branch_b,
            },
            condition="outer_route",
        )

    @step
    def z_branch_a(self):
        self.inner_route = "x_sub_a"
        self.next(
            {
                "x_sub_a": self.x_sub_a,
                "x_sub_b": self.x_sub_b,
            },
            condition="inner_route",
        )

    @step
    def x_sub_a(self):
        self.next(self.inner_join)

    @step
    def x_sub_b(self):
        self.next(self.inner_join)

    @step
    def inner_join(self):
        self.next(self.outer_join)

    @step
    def z_branch_b(self):
        self.next(self.outer_join)

    @step
    def outer_join(self):
        self.next(self.end)

    @step
    def end(self):
        pass


# Simple single-switch flow for regression.


class SimpleSwitchFlow(FlowSpec):
    @step
    def start(self):
        self.route = "left"
        self.next(
            {"left": self.left, "right": self.right},
            condition="route",
        )

    @step
    def left(self):
        self.next(self.join)

    @step
    def right(self):
        self.next(self.join)

    @step
    def join(self):
        self.next(self.end)

    @step
    def end(self):
        pass


# ── Fixtures ─────────────────────────────────────────────────────────────────


def _make_argo(mocker, flow_cls, name):
    mocker.patch.object(ArgoWorkflows, "_compile_workflow_template", return_value=None)
    mocker.patch.object(ArgoWorkflows, "_compile_sensor", return_value=None)
    return ArgoWorkflows(
        name=name,
        graph=flow_cls._graph,
        flow=flow_cls(use_cli=False),
        code_package_metadata={},
        code_package_sha="sha",
        code_package_url="s3://metaflow/test",
        production_token="token",
        metadata=None,
        flow_datastore=None,
        environment=None,
        event_logger=None,
        monitor=None,
        username="test-user",
    )


@pytest.fixture
def nested_alpha_argo(mocker):
    return _make_argo(mocker, NestedSwitchAlphaFlow, "nested-alpha")


@pytest.fixture
def nested_reverse_argo(mocker):
    return _make_argo(mocker, NestedSwitchReverseFlow, "nested-reverse")


@pytest.fixture
def simple_switch_argo(mocker):
    return _make_argo(mocker, SimpleSwitchFlow, "simple-switch")


# ── Tests ────────────────────────────────────────────────────────────────────


def test_nested_switch_alpha_order(nested_alpha_argo):
    """Bug repro: outer sorts before inner → inner branch nodes must still
    be conditional, and joins must be conditional joins."""
    aw = nested_alpha_argo

    # Inner branch nodes are conditional
    for name in ("b_sub_a", "b_sub_b"):
        assert name in aw.conditional_nodes, f"{name} should be in conditional_nodes"

    # inner_join is a conditional join
    assert "inner_join" in aw.conditional_join_nodes

    # outer_join is a conditional join
    assert "outer_join" in aw.conditional_join_nodes

    # matching_conditional_join_dict maps switches to their joins
    assert "start" in aw.matching_conditional_join_dict
    assert aw.matching_conditional_join_dict["start"] == "outer_join"

    assert "a_branch_a" in aw.matching_conditional_join_dict
    assert aw.matching_conditional_join_dict["a_branch_a"] == "inner_join"


def test_nested_switch_reverse_order(nested_reverse_argo):
    """Same topology with reversed names — should also pass."""
    aw = nested_reverse_argo

    for name in ("x_sub_a", "x_sub_b"):
        assert name in aw.conditional_nodes

    assert "inner_join" in aw.conditional_join_nodes
    assert "outer_join" in aw.conditional_join_nodes

    assert "start" in aw.matching_conditional_join_dict
    assert aw.matching_conditional_join_dict["start"] == "outer_join"

    assert "z_branch_a" in aw.matching_conditional_join_dict
    assert aw.matching_conditional_join_dict["z_branch_a"] == "inner_join"


def test_simple_switch_regression(simple_switch_argo):
    """Single switch must still work correctly."""
    aw = simple_switch_argo

    for name in ("left", "right"):
        assert name in aw.conditional_nodes

    assert "join" in aw.conditional_join_nodes

    assert "start" in aw.matching_conditional_join_dict
    assert aw.matching_conditional_join_dict["start"] == "join"
