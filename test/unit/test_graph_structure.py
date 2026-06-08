"""Tests for structural inference of start/end steps in FlowGraph.

Verifies that:
- Start step is determined by zero in-edges, end step by zero out-edges
- Steps can have any name (not just "start"/"end")
- Single-step flows are supported (start == end)
- Node types are assigned correctly based on structure
- _graph_info contains start_step/end_step fields
- Lint validation works with structural inference
- @step(start=True) / @step(end=True) annotations work correctly
"""

import pytest

from metaflow import Config, FlowMutator, FlowSpec, Parameter, resources, retry, step
from metaflow.flowspec import FlowStateItems
from metaflow.lint import LintWarn, linter

# ---------------------------------------------------------------------------
# Flow Definitions for Testing
# ---------------------------------------------------------------------------


class StandardFlow(FlowSpec):
    @step
    def start(self):
        self.x = 1
        self.next(self.end)

    @step
    def end(self):
        pass


class CustomNamedLinearFlow(FlowSpec):
    @step(start=True)
    def begin(self):
        self.x = 1
        self.next(self.middle)

    @step
    def middle(self):
        self.x += 1
        self.next(self.finish)

    @step(end=True)
    def finish(self):
        pass


class SingleStepFlow(FlowSpec):
    @step(start=True, end=True)
    def only(self):
        self.x = 42


class CustomNamedBranchFlow(FlowSpec):
    @step(start=True)
    def entry(self):
        self.next(self.a, self.b)

    @step
    def a(self):
        self.next(self.merge)

    @step
    def b(self):
        self.next(self.merge)

    @step
    def merge(self, inputs):
        self.next(self.done)

    @step(end=True)
    def done(self):
        pass


class CustomNamedForeachFlow(FlowSpec):
    @step(start=True)
    def init(self):
        self.items = [1, 2, 3]
        self.next(self.process, foreach="items")

    @step
    def process(self):
        self.result = self.input * 2
        self.next(self.collect)

    @step
    def collect(self, inputs):
        self.results = [i.result for i in inputs]
        self.next(self.final)

    @step(end=True)
    def final(self):
        pass


class SplitStartFlow(FlowSpec):
    """Start step that is also a split."""

    @step(start=True)
    def origin(self):
        self.next(self.left, self.right)

    @step
    def left(self):
        self.next(self.rejoin)

    @step
    def right(self):
        self.next(self.rejoin)

    @step
    def rejoin(self, inputs):
        self.next(self.terminus)

    @step(end=True)
    def terminus(self):
        pass


# ---------------------------------------------------------------------------
# Composed Flow Variations (Configs, Decorators, Mutators)
# ---------------------------------------------------------------------------


class _SingleStepWithConfig(FlowSpec):
    """Single-step flow utilizing a Config descriptor descriptor."""

    cfg = Config("cfg", default_value={"x": 7})

    @step(start=True, end=True)
    def only(self):
        self.v = self.cfg["x"]


class _SingleStepWithStackedDecos(FlowSpec):
    """Single-step flow with multiple stacked step decorators."""

    @retry(times=3)
    @resources(cpu=2, memory=1024)
    @step(start=True, end=True)
    def only(self):
        self.v = 1


class _AddRetryMutator(FlowMutator):
    """Appends @retry to every step to verify mutators target single-step topologies."""

    def pre_mutate(self, mutable_flow):
        for _, s in mutable_flow.steps:
            s.add_decorator(retry, deco_kwargs={"times": 1})


@_AddRetryMutator
class _SingleStepWithFlowMutator(FlowSpec):
    """Single-step flow with a class-level FlowMutator applied."""

    @step(start=True, end=True)
    def only(self):
        pass


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(
    params=[
        (StandardFlow, "start", "end"),
        (CustomNamedLinearFlow, "begin", "finish"),
        (SingleStepFlow, "only", "only"),
        (CustomNamedBranchFlow, "entry", "done"),
        (CustomNamedForeachFlow, "init", "final"),
        (SplitStartFlow, "origin", "terminus"),
    ],
    ids=[
        "standard",
        "custom-linear",
        "single-step",
        "branch",
        "foreach",
        "split-start",
    ],
)
def flow_with_endpoints(request):
    """Yield pairs of (flow_class, expected_start, expected_end) across topologies."""
    return request.param


# ---------------------------------------------------------------------------
# Tests: Structural Inference & Node Properties
# ---------------------------------------------------------------------------


def test_start_end_inference(flow_with_endpoints):
    """Verify standard and custom-named start/end step resolution properties."""
    flow_cls, expected_start, expected_end = flow_with_endpoints
    graph = flow_cls._graph
    assert graph.start_step == expected_start
    assert graph.end_step == expected_end


def test_standard_flow_types():
    """Verify standard start/end node type assignments."""
    graph = StandardFlow._graph
    assert graph["start"].type == "start"
    assert graph["end"].type == "end"


def test_custom_linear_types():
    """Verify linear flow node type mapping for custom-named steps."""
    graph = CustomNamedLinearFlow._graph
    assert graph["begin"].type == "start"
    assert graph["middle"].type == "linear"
    assert graph["finish"].type == "end"


def test_single_step_type_is_end():
    """Verify single-step flows default type to 'end' because they are terminal."""
    graph = SingleStepFlow._graph
    assert graph["only"].type == "end"


def test_branch_entry_is_split():
    """Ensure an entry step that acts as a split preserves its 'split' type identity."""
    graph = CustomNamedBranchFlow._graph
    assert graph["entry"].type == "split"
    assert graph["merge"].type == "join"
    assert graph["done"].type == "end"


def test_split_start_keeps_split_type():
    """Ensure start steps that split preserve the 'split' type to prevent linter balance errors."""
    graph = SplitStartFlow._graph
    assert graph["origin"].type == "split"


def test_foreach_entry_keeps_foreach_type():
    """Ensure structural routing correctly flags foreach entry and join steps."""
    graph = CustomNamedForeachFlow._graph
    assert graph["init"].type == "foreach"
    assert graph["collect"].type == "join"
    assert graph["final"].type == "end"


def test_custom_flow_in_funcs_out_funcs():
    """Verify in_funcs and out_funcs structural list accuracy."""
    graph = CustomNamedLinearFlow._graph
    assert graph["begin"].in_funcs == []
    assert graph["begin"].out_funcs == ["middle"]
    assert graph["finish"].in_funcs == ["middle"]
    assert graph["finish"].out_funcs == []


# ---------------------------------------------------------------------------
# Tests: Graph Output Structures & Serialization
# ---------------------------------------------------------------------------


def test_standard_graph_structure():
    """Verify default graph output maps cleanly to dictionary schemas."""
    steps_info, graph_structure = StandardFlow._graph.output_steps()
    assert graph_structure == ["start", "end"]
    assert set(steps_info.keys()) == {"start", "end"}


def test_custom_linear_graph_structure():
    """Verify custom-named linear flow serializes sequence steps in order."""
    steps_info, graph_structure = CustomNamedLinearFlow._graph.output_steps()
    assert graph_structure == ["begin", "middle", "finish"]
    assert set(steps_info.keys()) == {"begin", "middle", "finish"}


def test_single_step_graph_structure():
    """Verify singular flow graphs produce single-element output arrays."""
    steps_info, graph_structure = SingleStepFlow._graph.output_steps()
    assert graph_structure == ["only"]
    assert set(steps_info.keys()) == {"only"}


def test_branch_graph_structure():
    """Verify branching structures encapsulate interior paths inside endpoints."""
    steps_info, graph_structure = CustomNamedBranchFlow._graph.output_steps()
    assert graph_structure[0] == "entry"
    assert graph_structure[-1] == "done"
    assert "merge" in graph_structure
    assert set(steps_info.keys()) == {"entry", "a", "b", "merge", "done"}


def test_steps_info_types_match():
    """Ensure step metadata mapping values match underlying node_to_type structures."""
    steps_info, _ = CustomNamedLinearFlow._graph.output_steps()
    assert steps_info["begin"]["type"] == "start"
    assert steps_info["middle"]["type"] == "linear"
    assert steps_info["finish"]["type"] == "end"


def test_split_start_type_in_steps_info():
    """Ensure step information outputs map combined 'split-static' labels appropriately."""
    steps_info, _ = SplitStartFlow._graph.output_steps()
    assert steps_info["origin"]["type"] == "split-static"
    assert steps_info["terminus"]["type"] == "end"


def test_steps_info_has_next():
    """Verify 'next' step arrays accurately depict downstream paths."""
    steps_info, _ = CustomNamedLinearFlow._graph.output_steps()
    assert steps_info["begin"]["next"] == ["middle"]
    assert steps_info["middle"]["next"] == ["finish"]
    assert steps_info["finish"]["next"] == []


# ---------------------------------------------------------------------------
# Tests: Topological Sorting
# ---------------------------------------------------------------------------


def test_standard_sort():
    assert StandardFlow._graph.sorted_nodes == ["start", "end"]


def test_custom_linear_sort():
    assert CustomNamedLinearFlow._graph.sorted_nodes == ["begin", "middle", "finish"]


def test_single_step_sort():
    assert SingleStepFlow._graph.sorted_nodes == ["only"]


def test_branch_sort_order():
    """Verify sort topologies systematically anchor start first and end last."""
    graph = CustomNamedBranchFlow._graph
    assert graph.sorted_nodes[0] == "entry"
    assert graph.sorted_nodes[-1] == "done"


# ---------------------------------------------------------------------------
# Tests: Linting and Sanity Checks
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "flow_cls",
    [
        StandardFlow,
        CustomNamedLinearFlow,
        SingleStepFlow,
        CustomNamedBranchFlow,
        CustomNamedForeachFlow,
        SplitStartFlow,
    ],
    ids=[
        "standard",
        "custom-linear",
        "single-step",
        "branch",
        "foreach",
        "split-start",
    ],
)
def test_flow_passes_lint(flow_cls):
    """Verify that all standard test flow configurations cleanly pass validation checks."""
    linter.run_checks(flow_cls._graph)


# ---------------------------------------------------------------------------
# Tests: Node Info Meta-attributes
# ---------------------------------------------------------------------------


class _NodeInfoFlow(FlowSpec):
    @step(start=True, node_info={"label": "entry point", "priority": 1})
    def begin(self):
        self.next(self.end)

    @step(end=True)
    def end(self):
        pass


class _NestedNodeInfoFlow(FlowSpec):
    @step(start=True, node_info={"tags": ["gpu", "heavy"], "config": {"timeout": 300}})
    def begin(self):
        self.next(self.end)

    @step(end=True, node_info={})
    def end(self):
        pass


def test_node_info_stored_on_dag_node():
    graph = _NodeInfoFlow._graph
    assert graph["begin"].node_info == {"label": "entry point", "priority": 1}


def test_node_info_default_is_none_or_empty():
    graph = StandardFlow._graph
    assert graph["start"].node_info is None or graph["start"].node_info == {}


def test_node_info_appears_in_output_steps():
    steps_info, _ = _NodeInfoFlow._graph.output_steps()
    assert steps_info["begin"]["node_info"] == {"label": "entry point", "priority": 1}


def test_node_info_nested_dict():
    graph = _NestedNodeInfoFlow._graph
    assert graph["begin"].node_info == {
        "tags": ["gpu", "heavy"],
        "config": {"timeout": 300},
    }


def test_node_info_nested_in_output_steps():
    steps_info, _ = _NestedNodeInfoFlow._graph.output_steps()
    assert steps_info["begin"]["node_info"]["tags"] == ["gpu", "heavy"]
    assert steps_info["begin"]["node_info"]["config"]["timeout"] == 300


def test_node_info_empty_dict():
    graph = _NestedNodeInfoFlow._graph
    assert graph["end"].node_info == {}
    steps_info, _ = _NestedNodeInfoFlow._graph.output_steps()
    assert steps_info["end"]["node_info"] == {}


def test_node_info_absent_step_in_output_steps():
    """Steps lacking explicit node_info attributes return empty structures in serialized paths."""
    steps_info, _ = _NodeInfoFlow._graph.output_steps()
    end_info = steps_info["end"]["node_info"]
    assert end_info is None or end_info == {}


# ---------------------------------------------------------------------------
# Tests: Step Annotation Behaviors
# ---------------------------------------------------------------------------


def test_plain_step_has_no_annotations():
    """Verify standard decorators set default marker positions to False."""
    graph = StandardFlow._graph
    assert graph["start"].is_start_step is False
    assert graph["start"].is_end_step is False
    assert graph["end"].is_start_step is False
    assert graph["end"].is_end_step is False


def test_annotated_step_flags():
    """Verify start=True and end=True configure internal targeting flags properly."""
    graph = CustomNamedLinearFlow._graph
    assert graph["begin"].is_start_step is True
    assert graph["begin"].is_end_step is False
    assert graph["finish"].is_start_step is False
    assert graph["finish"].is_end_step is True
    assert graph["middle"].is_start_step is False
    assert graph["middle"].is_end_step is False


def test_annotated_single_step():
    """Verify single-step flows marking both parameters concurrently register successfully."""
    graph = SingleStepFlow._graph
    assert graph["only"].is_start_step is True
    assert graph["only"].is_end_step is True
    assert graph.start_step == "only"
    assert graph.end_step == "only"


def test_source_backed_single_step_with_next_still_fails_lint():
    """Ensure loops targeting single-step configurations throw lint validation errors."""

    class BadSingleStepFlow(FlowSpec):
        @step(start=True, end=True)
        def only(self):
            self.next(self.only)

    with pytest.raises(LintWarn):
        linter.run_checks(BadSingleStepFlow._graph)


def test_mixed_annotated_start_named_end():
    """Verify explicit start definitions combine with automated fallback terminal tracking."""

    class MixedFlow(FlowSpec):
        @step(start=True)
        def begin(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    graph = MixedFlow._graph
    assert graph.start_step == "begin"
    assert graph.end_step == "end"
    assert graph["begin"].is_start_step is True
    assert graph["end"].is_end_step is False


def test_backward_compat_name_based():
    """Ensure standard name-string fallback mechanics maintain legacy compliance constraints."""
    graph = StandardFlow._graph
    assert graph.start_step == "start"
    assert graph.end_step == "end"
    assert graph["start"].is_start_step is False
    assert graph["end"].is_end_step is False


# ---------------------------------------------------------------------------
# Tests: Advanced Object & Lifecycle Composition
# ---------------------------------------------------------------------------


def test_single_step_with_config_descriptor_registered():
    """Verify config tracking metrics link successfully to unified pipelines."""
    graph = _SingleStepWithConfig._graph
    assert graph.start_step == "only" == graph.end_step
    names = {name for name, _ in _SingleStepWithConfig._get_parameters()}
    assert "cfg" in names


def test_single_step_with_multiple_step_decorators():
    """Verify multi-tier runtime decorator combinations align correctly on simple targets."""
    graph = _SingleStepWithStackedDecos._graph
    deco_names = {deco.name for deco in graph["only"].decorators}
    assert {"retry", "resources"}.issubset(deco_names)


def test_single_step_with_flow_mutator_registered():
    """Verify class-level mutators link successfully to simple pipeline tracking arrays."""
    flow_cls = _SingleStepWithFlowMutator._flow_cls
    graph = flow_cls._graph
    assert graph.start_step == "only" == graph.end_step
    mutators = flow_cls._flow_state[FlowStateItems.FLOW_MUTATORS]
    assert any(isinstance(m, _AddRetryMutator) for m in mutators)


# ---------------------------------------------------------------------------
# Negative-Path Test Factories & Edge Cases
# ---------------------------------------------------------------------------


def _make_multiple_start():
    class MultipleStartFlow(FlowSpec):
        @step(start=True)
        def begin(self):
            self.next(self.end)

        @step(start=True)
        def also_begin(self):
            self.next(self.end)

        @step
        def end(self):
            pass

    return MultipleStartFlow


def _make_multiple_end():
    class MultipleEndFlow(FlowSpec):
        @step
        def start(self):
            self.next(self.done)

        @step(end=True)
        def done(self):
            pass

        @step(end=True)
        def also_done(self):
            pass

    return MultipleEndFlow


def _make_no_start():
    class NoStartFlow(FlowSpec):
        @step
        def compute(self):
            self.next(self.done)

        @step(end=True)
        def done(self):
            pass

    return NoStartFlow


def _make_no_end():
    class NoEndFlow(FlowSpec):
        @step(start=True)
        def begin(self):
            self.next(self.compute)

        @step
        def compute(self):
            pass

    return NoEndFlow


def _lint_warnings(flow_cls):
    """Run validation checks and capture thrown warnings."""
    graph = flow_cls._graph
    warnings = []
    for rule in linter._checks:
        try:
            rule(graph)
        except LintWarn as e:
            warnings.append(str(e))
    return graph, warnings


@pytest.mark.parametrize(
    "flow_factory, expected_none_field",
    [
        (_make_multiple_start, "start_step"),
        (_make_multiple_end, "end_step"),
        (_make_no_start, "start_step"),
        (_make_no_end, "end_step"),
    ],
    ids=["multiple-start", "multiple-end", "no-start", "no-end"],
)
def test_malformed_flow_sets_none(flow_factory, expected_none_field):
    """Verify that ambiguous or malformed pipeline layouts clear structural pointers to None."""
    graph = flow_factory()._graph
    assert getattr(graph, expected_none_field) is None


@pytest.mark.parametrize(
    "flow_factory, match_pattern",
    [
        (_make_multiple_start, "start"),
        (_make_multiple_end, "end"),
        (_make_no_start, "start"),
        (_make_no_end, "end"),
    ],
    ids=["multiple-start-lint", "multiple-end-lint", "no-start-lint", "no-end-lint"],
)
def test_malformed_flow_caught_by_lint(flow_factory, match_pattern):
    """Verify lint execution blocks invalid start/end variations with clear error contexts."""
    _, warnings = _lint_warnings(flow_factory())
    combined = " ".join(warnings).lower()
    assert (
        match_pattern in combined
    ), f"Expected lint warning about '{match_pattern}', got: {warnings}"
