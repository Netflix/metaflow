import pytest

from metaflow import FlowSpec, step
from metaflow.flowspec import InvalidNextException
from metaflow.lint import LintWarn, linter


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
