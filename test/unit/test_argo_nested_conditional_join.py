"""
Conditional (split-switch) join dependency regression tests.

See https://github.com/Netflix/metaflow/issues/3334. These tests assert
on the actual generated Argo `depends` string, not just on the
intermediate `conditional_nodes` / `conditional_join_nodes` /
`matching_conditional_join_dict` bookkeeping — the bookkeeping can look
right while the emitted `depends` field does not match the wrapper
contract used by the deployed workflow.
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


class RecursiveSwitchInForeachFlow(FlowSpec):
    @step
    def start(self):
        self.items = ["item"]
        self.next(self.loop, foreach="items")

    @step
    def loop(self):
        self.route = "done"
        self.next({"done": self.after_loop, "again": self.loop}, condition="route")

    @step
    def after_loop(self):
        self.next(self.join)

    @step
    def join(self, inputs):
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


def _depends(aw, node_name):
    """Return the Argo `depends` string generated for a given step name."""
    templates = aw._dag_templates()
    dag = templates[-1].payload["dag"]
    sanitized = ArgoWorkflows._sanitize(node_name)
    for task in dag["tasks"]:
        if task["name"] == sanitized:
            return task.get("depends", "")
    raise AssertionError(f"no DAG task found for step {node_name!r}")


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


@pytest.fixture
def recursive_switch_in_foreach_argo(mocker):
    return _make_argo(
        mocker, RecursiveSwitchInForeachFlow, "recursive-switch-in-foreach"
    )


# ── Tests ────────────────────────────────────────────────────────────────────


def test_nested_switch_alpha_order(nested_alpha_argo):
    aw = nested_alpha_argo

    for name in ("b_sub_a", "b_sub_b"):
        assert name in aw.conditional_nodes, f"{name} should be in conditional_nodes"

    assert "inner_join" in aw.conditional_join_nodes
    assert "outer_join" in aw.conditional_join_nodes

    assert aw.matching_conditional_join_dict["start"] == "outer_join"
    assert aw.matching_conditional_join_dict["a_branch_a"] == "inner_join"

    assert _depends(aw, "inner_join") == "b-sub-a.Succeeded && b-sub-b.Succeeded"
    assert _depends(aw, "outer_join") == "a-branch-b.Succeeded && inner-join.Succeeded"


def test_nested_switch_reverse_order(nested_reverse_argo):
    aw = nested_reverse_argo

    for name in ("x_sub_a", "x_sub_b"):
        assert name in aw.conditional_nodes

    assert "inner_join" in aw.conditional_join_nodes
    assert "outer_join" in aw.conditional_join_nodes

    assert aw.matching_conditional_join_dict["start"] == "outer_join"
    assert aw.matching_conditional_join_dict["z_branch_a"] == "inner_join"

    assert _depends(aw, "inner_join") == "x-sub-a.Succeeded && x-sub-b.Succeeded"
    assert _depends(aw, "outer_join") == "inner-join.Succeeded && z-branch-b.Succeeded"


def test_simple_switch_regression(simple_switch_argo):
    aw = simple_switch_argo

    for name in ("left", "right"):
        assert name in aw.conditional_nodes

    assert "join" in aw.conditional_join_nodes
    assert aw.matching_conditional_join_dict["start"] == "join"

    assert _depends(aw, "join") == "left.Succeeded && right.Succeeded"


def test_sequential_switch_regression(sequential_switch_argo):
    aw = sequential_switch_argo

    assert aw.matching_conditional_join_dict["join1"] == "join2"

    assert _depends(aw, "join1") == "left.Succeeded && right.Succeeded"
    assert _depends(aw, "join2") == "down.Succeeded && up.Succeeded"


def test_recursive_switch_join_is_wrapped(recursive_switch_argo):
    aw = recursive_switch_argo

    assert "step_b_loop" in aw.recursive_nodes
    assert aw.matching_conditional_join_dict["start"] == "merge"

    assert _depends(aw, "merge") == "shortcut.Succeeded && step-c.Succeeded"

    templates = aw._dag_templates()
    by_name = {template.payload["name"]: template.payload for template in templates}
    wrapper = by_name["step-b-loop"]
    driver = by_name["cond-step-b-loop"]
    assert wrapper["steps"][0][0]["template"] == "cond-step-b-loop"
    assert driver["steps"][1][0]["template"] == "cond-step-b-loop"
    assert driver["inputs"]["parameters"] == [{"name": "input-paths"}]
    assert by_name["step-c"]["steps"][0][0]["when"] == (
        "({{inputs.parameters.switch-step-value-step-b-loop}} == step_c && "
        "{{inputs.parameters.should-run-step-b-loop}} == true)"
    )

    outputs = {
        parameter["name"]: parameter["valueFrom"]["expression"]
        for parameter in driver["outputs"]["parameters"]
    }
    assert outputs["task-id"] == (
        "steps['step-b-loop-recursion']?.status == 'Succeeded'"
        " ? steps['step-b-loop-recursion'].outputs.parameters['task-id']"
        " : steps['step-b-loop-internal'].outputs.parameters['task-id']"
    )
    assert outputs["switch-step"] == (
        "steps['step-b-loop-recursion']?.status == 'Succeeded'"
        " ? steps['step-b-loop-recursion'].outputs.parameters['switch-step']"
        " : steps['step-b-loop-internal'].outputs.parameters['switch-step']"
    )


def test_recursive_switch_in_foreach_driver_and_exit_condition(
    recursive_switch_in_foreach_argo,
):
    templates = recursive_switch_in_foreach_argo._dag_templates()
    by_name = {template.payload["name"]: template.payload for template in templates}
    driver = by_name["loop"]

    assert driver["inputs"]["parameters"] == [
        {"name": "input-paths"},
        {"name": "split-index"},
    ]
    assert driver["steps"][0][0]["arguments"]["parameters"] == [
        {
            "name": "input-paths",
            "value": "{{inputs.parameters.input-paths}}",
        },
        {
            "name": "split-index",
            "value": "{{inputs.parameters.split-index}}",
        },
    ]
    assert driver["steps"][1][0]["arguments"]["parameters"] == [
        {
            "name": "input-paths",
            "value": (
                "argo-{{workflow.name}}/loop/"
                "{{steps.loop-internal.outputs.parameters.task-id}}"
            ),
        },
        {
            "name": "split-index",
            "value": "{{inputs.parameters.split-index}}",
        },
    ]

    foreach = by_name["start-foreach-items"]
    after_loop = next(
        task for task in foreach["dag"]["tasks"] if task["name"] == "after-loop"
    )
    assert after_loop["depends"] == "loop.Succeeded"
    assert (
        after_loop["when"]
        == "{{tasks.loop.outputs.parameters.switch-step}}==after_loop"
    )
