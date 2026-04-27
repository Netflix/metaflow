"""
Tests for structural inference of start/end steps in FlowGraph.

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
from metaflow import Config, FlowMutator, FlowSpec, step, Parameter, retry, resources
from metaflow.flowspec import FlowStateItems
from metaflow.lint import linter, LintWarn

# ---------------------------------------------------------------------------
# Flow definitions for testing
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


def _make_dynamic_single_step_flow():
    namespace = {}
    exec(
        compile("def only(self):\n    self.x = 42\n", "<synthetic>", "exec"), namespace
    )
    only = step(start=True, end=True)(namespace["only"])
    return type(
        "DynamicSingleStepFlow",
        (FlowSpec,),
        {"__module__": __name__, "only": only},
    )


# ---------------------------------------------------------------------------
# Flow classes: single-step flows composed with configs, decorators, mutators
# ---------------------------------------------------------------------------


class _SingleStepWithConfig(FlowSpec):
    """Single-step flow with a Config descriptor."""

    cfg = Config("cfg", default_value={"x": 7})

    @step(start=True, end=True)
    def only(self):
        self.v = self.cfg["x"]


class _SingleStepWithStackedDecos(FlowSpec):
    """Single-step flow with multiple step decorators stacked."""

    @retry(times=3)
    @resources(cpu=2, memory=1024)
    @step(start=True, end=True)
    def only(self):
        self.v = 1


class _AddRetryMutator(FlowMutator):
    """Adds @retry to every step. Used to verify mutators reach a single-step flow."""

    def pre_mutate(self, mutable_flow):
        for _, s in mutable_flow.steps:
            s.add_decorator(retry, deco_kwargs={"times": 1})


@_AddRetryMutator
class _SingleStepWithFlowMutator(FlowSpec):
    """Single-step flow with a FlowMutator applied at the class level."""

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
        "custom_linear",
        "single_step",
        "branch",
        "foreach",
        "split_start",
    ],
)
def flow_with_endpoints(request):
    """Yields (flow_class, expected_start, expected_end) for each topology."""
    return request.param


# ---------------------------------------------------------------------------
# Tests: Structural inference
# ---------------------------------------------------------------------------


def test_start_end_inference(flow_with_endpoints):
    flow_cls, expected_start, expected_end = flow_with_endpoints
    graph = flow_cls._graph
    assert graph.start_step == expected_start
    assert graph.end_step == expected_end


# ---------------------------------------------------------------------------
# Tests: Node types
# ---------------------------------------------------------------------------


def test_standard_flow_types():
    graph = StandardFlow._graph
    assert graph["start"].type == "start"
    assert graph["end"].type == "end"


def test_custom_linear_types():
    graph = CustomNamedLinearFlow._graph
    assert graph["begin"].type == "start"
    assert graph["middle"].type == "linear"
    assert graph["finish"].type == "end"


def test_single_step_type_is_end():
    """Single-step flow: type is 'end' since it's terminal."""
    graph = SingleStepFlow._graph
    assert graph["only"].type == "end"


def test_branch_entry_is_split():
    """Entry step that splits should keep 'split' type, not be overridden to 'start'."""
    graph = CustomNamedBranchFlow._graph
    assert graph["entry"].type == "split"
    assert graph["merge"].type == "join"
    assert graph["done"].type == "end"


def test_split_start_keeps_split_type():
    """Start step that is also a split must keep 'split' type for lint balance."""
    graph = SplitStartFlow._graph
    assert graph["origin"].type == "split"


def test_foreach_entry_keeps_foreach_type():
    graph = CustomNamedForeachFlow._graph
    assert graph["init"].type == "foreach"
    assert graph["collect"].type == "join"
    assert graph["final"].type == "end"


def test_custom_flow_in_funcs_out_funcs():
    graph = CustomNamedLinearFlow._graph
    assert graph["begin"].in_funcs == []
    assert graph["begin"].out_funcs == ["middle"]
    assert graph["finish"].in_funcs == ["middle"]
    assert graph["finish"].out_funcs == []


# ---------------------------------------------------------------------------
# Tests: output_steps / graph_structure
# ---------------------------------------------------------------------------


def test_standard_graph_structure():
    steps_info, graph_structure = StandardFlow._graph.output_steps()
    assert graph_structure == ["start", "end"]
    assert set(steps_info.keys()) == {"start", "end"}


def test_custom_linear_graph_structure():
    steps_info, graph_structure = CustomNamedLinearFlow._graph.output_steps()
    assert graph_structure == ["begin", "middle", "finish"]
    assert set(steps_info.keys()) == {"begin", "middle", "finish"}


def test_single_step_graph_structure():
    steps_info, graph_structure = SingleStepFlow._graph.output_steps()
    assert graph_structure == ["only"]
    assert set(steps_info.keys()) == {"only"}


def test_branch_graph_structure():
    steps_info, graph_structure = CustomNamedBranchFlow._graph.output_steps()
    assert graph_structure[0] == "entry"
    assert graph_structure[-1] == "done"
    assert "merge" in graph_structure
    assert set(steps_info.keys()) == {"entry", "a", "b", "merge", "done"}


def test_steps_info_types_match():
    """Steps info type should match the node_to_type mapping."""
    steps_info, _ = CustomNamedLinearFlow._graph.output_steps()
    assert steps_info["begin"]["type"] == "start"
    assert steps_info["middle"]["type"] == "linear"
    assert steps_info["finish"]["type"] == "end"


def test_split_start_type_in_steps_info():
    """When start step is a split, steps_info should show split-static."""
    steps_info, _ = SplitStartFlow._graph.output_steps()
    assert steps_info["origin"]["type"] == "split-static"
    assert steps_info["terminus"]["type"] == "end"


def test_steps_info_has_next():
    steps_info, _ = CustomNamedLinearFlow._graph.output_steps()
    assert steps_info["begin"]["next"] == ["middle"]
    assert steps_info["middle"]["next"] == ["finish"]
    assert steps_info["finish"]["next"] == []


# ---------------------------------------------------------------------------
# Tests: Topological sort
# ---------------------------------------------------------------------------


def test_standard_sort():
    assert StandardFlow._graph.sorted_nodes == ["start", "end"]


def test_custom_linear_sort():
    assert CustomNamedLinearFlow._graph.sorted_nodes == ["begin", "middle", "finish"]


def test_single_step_sort():
    assert SingleStepFlow._graph.sorted_nodes == ["only"]


def test_branch_sort_order():
    """Start must come first, end must come last."""
    graph = CustomNamedBranchFlow._graph
    assert graph.sorted_nodes[0] == "entry"
    assert graph.sorted_nodes[-1] == "done"


# ---------------------------------------------------------------------------
# Tests: Lint validation
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
        "custom_linear",
        "single_step",
        "branch",
        "foreach",
        "split_start",
    ],
)
def test_flow_passes_lint(flow_cls):
    linter.run_checks(flow_cls._graph)


# ---------------------------------------------------------------------------
# Tests: node_info metadata
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
    """Steps without node_info should have None or {} in output_steps."""
    steps_info, _ = _NodeInfoFlow._graph.output_steps()
    end_info = steps_info["end"]["node_info"]
    assert end_info is None or end_info == {}


# ---------------------------------------------------------------------------
# Tests: Annotation mechanics
# ---------------------------------------------------------------------------


def test_plain_step_has_no_annotations():
    """Plain @step sets is_start_step=False and is_end_step=False."""
    graph = StandardFlow._graph
    assert graph["start"].is_start_step is False
    assert graph["start"].is_end_step is False
    assert graph["end"].is_start_step is False
    assert graph["end"].is_end_step is False


def test_annotated_step_flags():
    """@step(start=True) and @step(end=True) set the flags on the node."""
    graph = CustomNamedLinearFlow._graph
    assert graph["begin"].is_start_step is True
    assert graph["begin"].is_end_step is False
    assert graph["finish"].is_start_step is False
    assert graph["finish"].is_end_step is True
    assert graph["middle"].is_start_step is False
    assert graph["middle"].is_end_step is False


def test_annotated_single_step():
    """@step(start=True, end=True) single-step flow works."""
    graph = SingleStepFlow._graph
    assert graph["only"].is_start_step is True
    assert graph["only"].is_end_step is True
    assert graph.start_step == "only"
    assert graph.end_step == "only"


def test_dynamic_single_step_without_inspectable_source():
    """Dynamically-generated @step(start=True, end=True) works without source."""
    graph = _make_dynamic_single_step_flow()._graph

    assert graph.start_step == "only"
    assert graph.end_step == "only"
    assert graph["only"].type == "end"
    assert graph["only"].num_args == 1
    assert graph["only"].func_lineno == 1
    assert graph["only"].source_file == "<synthetic>"

    linter.run_checks(graph)


def test_source_backed_single_step_with_next_still_fails_lint():
    """Source-backed @step(start=True, end=True) with self.next() still fails lint."""

    class BadSingleStepFlow(FlowSpec):
        @step(start=True, end=True)
        def only(self):
            self.next(self.only)

    with pytest.raises(LintWarn):
        linter.run_checks(BadSingleStepFlow._graph)


def test_mixed_annotated_start_named_end():
    """Annotated start + name-based end fallback."""

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
    """Flow with just 'start'/'end' names still works (no annotations)."""
    graph = StandardFlow._graph
    assert graph.start_step == "start"
    assert graph.end_step == "end"
    assert graph["start"].is_start_step is False
    assert graph["end"].is_end_step is False


# ---------------------------------------------------------------------------
# Tests: composition with configs, stacked decorators, and flow mutators
# ---------------------------------------------------------------------------


def test_single_step_with_config_descriptor_registered():
    """Config descriptor is registered on a single-step flow."""
    graph = _SingleStepWithConfig._graph
    assert graph.start_step == "only" == graph.end_step
    names = {name for name, _ in _SingleStepWithConfig._get_parameters()}
    assert "cfg" in names


def test_single_step_with_multiple_step_decorators():
    """Multiple step decorators stack correctly on a single-step flow."""
    graph = _SingleStepWithStackedDecos._graph
    deco_names = {deco.name for deco in graph["only"].decorators}
    assert {"retry", "resources"}.issubset(deco_names)


def test_single_step_with_flow_mutator_registered():
    """FlowMutator is registered on a single-step flow at class-definition time.

    pre_mutate only fires when the flow is processed via the CLI layer, so
    the decorator it adds won't appear on the graph in a unit test. What we
    can verify here is that the mutator syntax is accepted by a single-step
    FlowSpec and that it's registered as a flow mutator. End-to-end execution
    is covered by the matching integration test.
    """
    flow_cls = _SingleStepWithFlowMutator._flow_cls
    graph = flow_cls._graph
    assert graph.start_step == "only" == graph.end_step
    mutators = flow_cls._flow_state[FlowStateItems.FLOW_MUTATORS]
    assert any(isinstance(m, _AddRetryMutator) for m in mutators)


# ---------------------------------------------------------------------------
# Negative-path tests: malformed annotation patterns caught by lint
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
    """Run all lint checks and collect LintWarn exceptions."""
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
    ids=["multiple_start", "multiple_end", "no_start", "no_end"],
)
def test_malformed_flow_sets_none(flow_factory, expected_none_field):
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
    ids=["multiple_start_lint", "multiple_end_lint", "no_start_lint", "no_end_lint"],
)
def test_malformed_flow_caught_by_lint(flow_factory, match_pattern):
    _, warnings = _lint_warnings(flow_factory())
    combined = " ".join(warnings).lower()
    assert match_pattern in combined, "Expected lint warning about '%s', got: %s" % (
        match_pattern,
        warnings,
    )
