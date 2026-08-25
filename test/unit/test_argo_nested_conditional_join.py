"""
Conditional (split-switch) join dependency regression tests.

See https://github.com/Netflix/metaflow/issues/3334. These tests assert
on the actual generated Argo `depends` string, not just on the
intermediate `conditional_nodes` / `conditional_join_nodes` /
`matching_conditional_join_dict` bookkeeping — the bookkeeping can look
right while the emitted `depends` field still uses `&&` between
mutually exclusive branches, which is what actually breaks deployed
workflows (the join gets stuck in `Omitted`).
"""

import pytest

from metaflow import FlowSpec, step
from metaflow.plugins.argo.argo_workflows import ArgoWorkflows


# ── Flows ────────────────────────────────────────────────────────────────────


class NestedSwitchAlphaFlow(FlowSpec):
    """Nested switches; inner switch (`a_branch_a`) sorts before outer
    (`start`) — the ordering that always worked."""

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


class NestedSwitchReverseFlow(FlowSpec):
    """Same topology, opposite sort order: outer (`start`) before inner
    (`z_branch_a`) — the ordering that triggered the bug."""

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


class SimpleSwitchFlow(FlowSpec):
    """Single switch, no nesting."""

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


class SequentialSwitchFlow(FlowSpec):
    """Two switches in sequence, not nested: switch -> join1 -> switch2
    -> join2."""

    @step
    def start(self):
        self.route = "left"
        self.next({"left": self.left, "right": self.right}, condition="route")

    @step
    def left(self):
        self.next(self.join1)

    @step
    def right(self):
        self.next(self.join1)

    @step
    def join1(self):
        self.route2 = "up"
        self.next({"up": self.up, "down": self.down}, condition="route2")

    @step
    def up(self):
        self.next(self.join2)

    @step
    def down(self):
        self.next(self.join2)

    @step
    def join2(self):
        self.next(self.end)

    @step
    def end(self):
        pass


class RecursiveSwitchJoinFlow(FlowSpec):
    """Minimal repro from issue #3334: a switch branch through a
    self-looping switch (`step_b_loop`) rejoins a direct branch at
    `merge`."""

    @step
    def start(self):
        self.use_shortcut = "yes"
        self.next(
            {"yes": self.shortcut, "no": self.long_path},
            condition="use_shortcut",
        )

    @step
    def shortcut(self):
        self.next(self.merge)

    @step
    def long_path(self):
        self.next(self.step_b_loop)

    @step
    def step_b_loop(self):
        self.should_continue = "no"
        self.next(
            {"yes": self.step_b_loop, "no": self.step_c},
            condition="should_continue",
        )

    @step
    def step_c(self):
        self.next(self.merge)

    @step
    def merge(self):
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
        # Avoid needing a real `environment` for the heartbeat daemon
        # template, which _dag_templates() would otherwise try to build.
        enable_heartbeat_daemon=False,
    )


def _task(aw, node_name):
    """Return the raw Argo DAG task dict generated for a given step name."""
    templates = aw._dag_templates()
    dag = templates[-1].payload["dag"]
    sanitized = ArgoWorkflows._sanitize(node_name)
    for task in dag["tasks"]:
        if task["name"] == sanitized:
            return task
    raise AssertionError(f"no DAG task found for step {node_name!r}")


def _depends(aw, node_name):
    """Return the Argo `depends` string generated for a given step name."""
    return _task(aw, node_name).get("depends", "")


def _when(aw, node_name):
    """Return the Argo `when` string generated for a given step name."""
    return _task(aw, node_name).get("when")


def _param(aw, node_name, param_name):
    """Return the value of an input parameter on a given step's DAG task."""
    task = _task(aw, node_name)
    for p in task["arguments"]["parameters"]:
        if p["name"] == param_name:
            return p["value"]
    raise AssertionError(f"no parameter {param_name!r} found for step {node_name!r}")


@pytest.fixture
def nested_alpha_argo(mocker):
    return _make_argo(mocker, NestedSwitchAlphaFlow, "nested-alpha")


@pytest.fixture
def nested_reverse_argo(mocker):
    return _make_argo(mocker, NestedSwitchReverseFlow, "nested-reverse")


@pytest.fixture
def simple_switch_argo(mocker):
    return _make_argo(mocker, SimpleSwitchFlow, "simple-switch")


@pytest.fixture
def sequential_switch_argo(mocker):
    return _make_argo(mocker, SequentialSwitchFlow, "sequential-switch")


@pytest.fixture
def recursive_switch_argo(mocker):
    return _make_argo(mocker, RecursiveSwitchJoinFlow, "recursive-switch")


# ── Tests ────────────────────────────────────────────────────────────────────


def test_nested_switch_alpha_order(nested_alpha_argo):
    aw = nested_alpha_argo

    for name in ("b_sub_a", "b_sub_b"):
        assert name in aw.conditional_nodes, f"{name} should be in conditional_nodes"

    assert "inner_join" in aw.conditional_join_nodes
    assert "outer_join" in aw.conditional_join_nodes

    assert aw.matching_conditional_join_dict["start"] == "outer_join"
    assert aw.matching_conditional_join_dict["a_branch_a"] == "inner_join"

    assert _depends(aw, "inner_join") == "b-sub-a.Succeeded || b-sub-b.Succeeded"
    assert _depends(aw, "outer_join") == "a-branch-b.Succeeded || inner-join.Succeeded"


def test_nested_switch_reverse_order(nested_reverse_argo):
    aw = nested_reverse_argo

    for name in ("x_sub_a", "x_sub_b"):
        assert name in aw.conditional_nodes

    assert "inner_join" in aw.conditional_join_nodes
    assert "outer_join" in aw.conditional_join_nodes

    assert aw.matching_conditional_join_dict["start"] == "outer_join"
    assert aw.matching_conditional_join_dict["z_branch_a"] == "inner_join"

    assert _depends(aw, "inner_join") == "x-sub-a.Succeeded || x-sub-b.Succeeded"
    assert _depends(aw, "outer_join") == "inner-join.Succeeded || z-branch-b.Succeeded"


def test_simple_switch_regression(simple_switch_argo):
    aw = simple_switch_argo

    for name in ("left", "right"):
        assert name in aw.conditional_nodes

    assert "join" in aw.conditional_join_nodes
    assert aw.matching_conditional_join_dict["start"] == "join"

    assert _depends(aw, "join") == "left.Succeeded || right.Succeeded"


def test_sequential_switch_regression(sequential_switch_argo):
    aw = sequential_switch_argo

    assert aw.matching_conditional_join_dict["join1"] == "join2"

    assert _depends(aw, "join1") == "left.Succeeded || right.Succeeded"
    assert _depends(aw, "join2") == "down.Succeeded || up.Succeeded"


def test_recursive_switch_join_depends_or(recursive_switch_argo):
    aw = recursive_switch_argo

    assert "step_b_loop" in aw.recursive_nodes
    assert aw.matching_conditional_join_dict["start"] == "merge"

    assert _depends(aw, "merge") == "shortcut.Succeeded || step-c.Succeeded"


# ── Regression tests ────────────────────────────────────────────────────────
#
# Conditional branch nodes are wrapped in a Steps template (see
# `_build_conditional_wrapper`) so that their `task-id` output always
# resolves, even when their branch is skipped. This means a wrapped node's
# own DAGTask always completes with status 'Succeeded', regardless of
# whether its `inner` step actually ran - so depends()'s `X.Succeeded`
# checks can no longer tell a join whether a conditional branch was really
# taken. A closing join whose conditional predecessors aren't themselves
# split-switch nodes (so it never got a `when` clause from the
# switch-based path) must instead gate on each predecessor's `should-run`
# output.


def test_nested_switch_alpha_closing_join_when_gate(nested_alpha_argo):
    aw = nested_alpha_argo

    assert _when(aw, "inner_join") is None  # gated inside its own wrapper instead
    when = _when(aw, "outer_join")
    assert when is not None
    assert "tasks['a-branch-b']?.outputs?.parameters['should-run'] == 'true'" in when
    assert "tasks['inner-join']?.outputs?.parameters['should-run'] == 'true'" in when


def test_simple_switch_closing_join_when_gate(simple_switch_argo):
    aw = simple_switch_argo

    when = _when(aw, "join")
    assert when is not None
    assert "tasks['left']?.outputs?.parameters['should-run'] == 'true'" in when
    assert "tasks['right']?.outputs?.parameters['should-run'] == 'true'" in when


def test_sequential_switch_closing_join_when_gate(sequential_switch_argo):
    aw = sequential_switch_argo

    # join1 is itself a wrapped conditional node (it also starts a second
    # split-switch), so it is gated inside its own wrapper's `inner` step.
    assert _when(aw, "join1") is None
    when = _when(aw, "join2")
    assert when is not None
    assert "tasks['down']?.outputs?.parameters['should-run'] == 'true'" in when
    assert "tasks['up']?.outputs?.parameters['should-run'] == 'true'" in when


def test_recursive_switch_closing_join_when_gate(recursive_switch_argo):
    aw = recursive_switch_argo

    when = _when(aw, "merge")
    assert when is not None
    assert "tasks['shortcut']?.outputs?.parameters['should-run'] == 'true'" in when
    assert "tasks['step-c']?.outputs?.parameters['should-run'] == 'true'" in when


def test_dual_join_switch_should_run_params_use_output_not_status(
    sequential_switch_argo,
):
    """join1 both closes the first switch's branches and opens a second one,
    so it is a wrapped conditional node whose `inner` step gating depends on
    should-run-left/should-run-right input parameters. Regardless of which
    of {left, right} the DAG walk finalizes join1's task from first, both
    parameters must reference the predecessor's should-run *output*, not a
    status fallback (which would be trivially true for wrapped nodes)."""
    aw = sequential_switch_argo

    assert _param(aw, "join1", "should-run-left") == (
        "{{tasks.left.outputs.parameters.should-run}}"
    )
    assert _param(aw, "join1", "should-run-right") == (
        "{{tasks.right.outputs.parameters.should-run}}"
    )
