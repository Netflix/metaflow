import os

import pytest

from metaflow import FlowSpec, Runner, step
from metaflow.flowspec import InvalidNextException
from metaflow.lint import LintWarn, linter
from metaflow.task import _switch_case_targets_for_input_steps


SWITCH_FANOUT_RUNTIME_FLOW_FILE = os.path.join(
    os.path.dirname(__file__), "flows", "switch_fanout_runtime_flow.py"
)


class SwitchFanoutCaseFlow(FlowSpec):
    @step
    def start(self):
        self.route = "miss"
        self.next(
            {
                "hit": [self.finalize, self.hit_second, self.hit_third],
                "miss": [self.clip, self.face],
            },
            condition="route",
        )

    @step
    def finalize(self):
        self.next(self.join_case)

    @step
    def hit_second(self):
        self.next(self.join_case)

    @step
    def hit_third(self):
        self.next(self.join_case)

    @step
    def clip(self):
        self.next(self.join_case)

    @step
    def face(self):
        self.next(self.join_case)

    @step
    def join_case(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


class SwitchFanoutToEndJoinFlow(FlowSpec):
    @step
    def start(self):
        self.route = "miss"
        self.next(
            {"hit": self.finalize, "miss": [self.clip, self.face]},
            condition="route",
        )

    @step
    def finalize(self):
        self.next(self.end)

    @step
    def clip(self):
        self.next(self.end)

    @step
    def face(self):
        self.next(self.end)

    @step
    def end(self, inputs):
        pass


class SwitchSharedScalarTargetFlow(FlowSpec):
    @step
    def start(self):
        self.route = "a"
        self.next({"a": self.shared, "b": self.shared}, condition="route")

    @step
    def shared(self):
        self.next(self.end)

    @step
    def end(self):
        pass


class SingleCaseSwitchFanoutFlow(FlowSpec):
    @step
    def start(self):
        self.route = "only"
        self.next({"only": [self.left, self.right]}, condition="route")

    @step
    def left(self):
        self.next(self.join)

    @step
    def right(self):
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


class SwitchFanoutOverlappingTargetsFlow(FlowSpec):
    @step
    def start(self):
        self.route = "b"
        self.next(
            {
                "a": [self.shared, self.a_only],
                "b": [self.shared, self.b_one, self.b_two],
            },
            condition="route",
        )

    @step
    def shared(self):
        self.next(self.join_case)

    @step
    def a_only(self):
        self.next(self.join_case)

    @step
    def b_one(self):
        self.next(self.join_case)

    @step
    def b_two(self):
        self.next(self.join_case)

    @step
    def join_case(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


def test_graph_parses_switch_fanout_case():
    graph = SwitchFanoutCaseFlow._graph

    assert graph["start"].type == "split-switch"
    assert graph["start"].switch_cases == {
        "hit": ["finalize", "hit_second", "hit_third"],
        "miss": ["clip", "face"],
    }
    assert graph["start"].out_funcs == [
        "finalize",
        "hit_second",
        "hit_third",
        "clip",
        "face",
    ]
    assert graph["clip"].split_parents == ["start"]
    assert graph["clip"].split_branches == ["clip"]
    assert graph["face"].split_parents == ["start"]
    assert graph["face"].split_branches == ["face"]
    assert graph["join_case"].type == "join"
    assert graph["join_case"].split_parents == ["start"]


def test_switch_fanout_case_passes_lint():
    linter.run_checks(SwitchFanoutCaseFlow._graph)


def test_switch_fanout_transition_uses_selected_case_targets():
    flow = SwitchFanoutCaseFlow(use_cli=False)
    flow._current_step = "start"
    flow.route = "miss"

    flow.next(
        {"hit": flow.finalize, "miss": [flow.clip, flow.face]},
        condition="route",
    )

    assert flow._transition == (["clip", "face"], None)


def test_switch_fanout_transition_rejects_empty_case():
    flow = SwitchFanoutCaseFlow(use_cli=False)
    flow._current_step = "start"
    flow.route = "miss"

    with pytest.raises(InvalidNextException, match="empty switch transition"):
        flow.next({"hit": flow.finalize, "miss": []}, condition="route")


def test_switch_fanout_case_cannot_join_at_terminal_step():
    with pytest.raises(LintWarn, match="terminal step .* should not be a join step"):
        linter.run_checks(SwitchFanoutToEndJoinFlow._graph)


def test_scalar_switch_cases_can_share_target():
    linter.run_checks(SwitchSharedScalarTargetFlow._graph)


def test_single_switch_case_is_rejected_even_when_it_has_multiple_targets():
    with pytest.raises(LintWarn, match="1 found, at least 2 required"):
        linter.run_checks(SingleCaseSwitchFanoutFlow._graph)


@pytest.mark.parametrize(
    "input_step_names, expected_targets",
    [
        (("clip",), ["clip", "face"]),
        (("clip", "face"), ["clip", "face"]),
        (("finalize", "hit_second"), ["finalize", "hit_second", "hit_third"]),
        (("clip", "finalize"), None),
        (("clip", "clip"), None),
    ],
    ids=[
        "missing-branch",
        "complete-case",
        "partial-wider-case",
        "mixed-cases",
        "duplicate-branch",
    ],
)
def test_task_resolves_switch_case_from_actual_input_branches(
    input_step_names, expected_targets
):
    assert (
        _switch_case_targets_for_input_steps(
            SwitchFanoutCaseFlow._graph,
            SwitchFanoutCaseFlow._graph["start"],
            input_step_names,
        )
        == expected_targets
    )


def test_switch_fanout_cases_cannot_share_targets():
    with pytest.raises(
        LintWarn,
        match="multi-target switch cases .* share target step.*shared",
    ):
        linter.run_checks(SwitchFanoutOverlappingTargetsFlow._graph)


@pytest.mark.parametrize("route, expected_count", [("a", 2), ("b", 3)])
def test_switch_fanout_executes_nested_splits_and_shared_join(
    tmp_path, route, expected_count
):
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env = {
        "METAFLOW_USER": "switch-fanout-test",
        "PYTHONPATH": os.pathsep.join(
            path for path in (repo_root, os.environ.get("PYTHONPATH")) if path
        ),
    }
    with Runner(
        SWITCH_FANOUT_RUNTIME_FLOW_FILE,
        show_output=False,
        cwd=str(tmp_path),
        env=env,
        datastore="local",
        metadata="local",
        file_read_timeout=60,
    ).run(route=route) as running:
        output = running.stdout + running.stderr
        assert running.returncode == 0, output
        assert "INPUT_COUNT=%d" % expected_count in output
