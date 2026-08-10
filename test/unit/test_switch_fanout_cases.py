import os
import subprocess
import sys
import textwrap

import pytest

from metaflow import FlowSpec, step
from metaflow.flowspec import InvalidNextException
from metaflow.lint import LintWarn, linter


RUNTIME_FLOW = r"""
from metaflow import FlowSpec, Parameter, step


class SwitchFanoutRuntimeFlow(FlowSpec):
    route = Parameter("route")

    @step
    def start(self):
        self.next(
            {"a": (self.a_split, self.a_foreach),
             "b": [self.b_one, self.b_two, self.b_three]},
            condition="route",
        )

    @step
    def a_split(self):
        self.next(self.a_left, self.a_right)

    @step
    def a_left(self):
        self.next(self.a_split_join)

    @step
    def a_right(self):
        self.next(self.a_split_join)

    @step
    def a_split_join(self, inputs):
        self.next(self.shared_join)

    @step
    def a_foreach(self):
        self.values = [1, 2]
        self.next(self.a_worker, foreach="values")

    @step
    def a_worker(self):
        self.next(self.a_foreach_join)

    @step
    def a_foreach_join(self, inputs):
        self.next(self.shared_join)

    @step
    def b_one(self):
        self.next(self.shared_join)

    @step
    def b_two(self):
        self.next(self.shared_join)

    @step
    def b_three(self):
        self.next(self.shared_join)

    @step
    def shared_join(self, inputs):
        self.input_count = sum(1 for _ in inputs)
        self.next(self.end)

    @step
    def end(self):
        print("INPUT_COUNT=%d" % self.input_count)


if __name__ == "__main__":
    SwitchFanoutRuntimeFlow()
"""


class SwitchFanoutCaseFlow(FlowSpec):
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
        self.next(self.join_miss)

    @step
    def face(self):
        self.next(self.join_miss)

    @step
    def join_miss(self, inputs):
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
        "hit": "finalize",
        "miss": ["clip", "face"],
    }
    assert graph["start"].out_funcs == ["finalize", "clip", "face"]
    assert graph["clip"].split_parents == ["start"]
    assert graph["clip"].split_branches == ["clip"]
    assert graph["face"].split_parents == ["start"]
    assert graph["face"].split_branches == ["face"]
    assert graph["join_miss"].type == "join"
    assert graph["join_miss"].split_parents == ["start"]


def test_switch_fanout_case_passes_lint():
    linter.run_checks(SwitchFanoutCaseFlow._graph)


def test_runtime_switch_fanout_transition_uses_selected_case_targets():
    flow = SwitchFanoutCaseFlow(use_cli=False)
    flow._current_step = "start"
    flow.route = "miss"

    flow.next(
        {"hit": flow.finalize, "miss": [flow.clip, flow.face]},
        condition="route",
    )

    assert flow._transition == (["clip", "face"], None)


def test_runtime_switch_fanout_rejects_empty_case():
    flow = SwitchFanoutCaseFlow(use_cli=False)
    flow._current_step = "start"
    flow.route = "miss"

    with pytest.raises(InvalidNextException, match="empty switch transition"):
        flow.next({"hit": flow.finalize, "miss": []}, condition="route")


def test_switch_fanout_case_cannot_join_at_terminal_step():
    with pytest.raises(LintWarn, match="terminal step .* should not be a join step"):
        linter.run_checks(SwitchFanoutToEndJoinFlow._graph)


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
    flow_file = tmp_path / "switch_fanout_runtime_flow.py"
    flow_file.write_text(textwrap.dedent(RUNTIME_FLOW))
    env = os.environ.copy()
    env["METAFLOW_USER"] = "switch-fanout-test"
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env["PYTHONPATH"] = os.pathsep.join(
        path for path in (repo_root, env.get("PYTHONPATH")) if path
    )

    result = subprocess.run(
        [
            sys.executable,
            str(flow_file),
            "--datastore=local",
            "--metadata=local",
            "run",
            "--route",
            route,
        ],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "INPUT_COUNT=%d" % expected_count in output
