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
from metaflow import FlowSpec, step, Parameter
from metaflow.lint import linter, LintWarn


# ---------------------------------------------------------------------------
# Flow definitions for testing (must be importable from a real file)
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
# Tests: Structural inference
# ---------------------------------------------------------------------------


class TestStartEndInference:
    """Tests for FlowGraph._identify_start_end()."""

    def test_standard_names(self):
        graph = StandardFlow._graph
        assert graph.start_step == "start"
        assert graph.end_step == "end"

    def test_custom_names_linear(self):
        graph = CustomNamedLinearFlow._graph
        assert graph.start_step == "begin"
        assert graph.end_step == "finish"

    def test_single_step(self):
        graph = SingleStepFlow._graph
        assert graph.start_step == "only"
        assert graph.end_step == "only"
        assert graph.start_step == graph.end_step

    def test_custom_names_branch(self):
        graph = CustomNamedBranchFlow._graph
        assert graph.start_step == "entry"
        assert graph.end_step == "done"

    def test_custom_names_foreach(self):
        graph = CustomNamedForeachFlow._graph
        assert graph.start_step == "init"
        assert graph.end_step == "final"

    def test_split_start(self):
        graph = SplitStartFlow._graph
        assert graph.start_step == "origin"
        assert graph.end_step == "terminus"


# ---------------------------------------------------------------------------
# Tests: Node types
# ---------------------------------------------------------------------------


class TestNodeTypes:
    """Tests that node types are assigned based on structure, not names."""

    def test_standard_flow_types(self):
        graph = StandardFlow._graph
        assert graph["start"].type == "start"
        assert graph["end"].type == "end"

    def test_custom_linear_types(self):
        graph = CustomNamedLinearFlow._graph
        assert graph["begin"].type == "start"
        assert graph["middle"].type == "linear"
        assert graph["finish"].type == "end"

    def test_single_step_type_is_end(self):
        """Single-step flow: type is 'end' since it's terminal."""
        graph = SingleStepFlow._graph
        assert graph["only"].type == "end"

    def test_branch_entry_is_split(self):
        """Entry step that splits should keep 'split' type, not be overridden to 'start'."""
        graph = CustomNamedBranchFlow._graph
        assert graph["entry"].type == "split"
        assert graph["merge"].type == "join"
        assert graph["done"].type == "end"

    def test_split_start_keeps_split_type(self):
        """Start step that is also a split must keep 'split' type for lint balance."""
        graph = SplitStartFlow._graph
        assert graph["origin"].type == "split"

    def test_foreach_entry_keeps_foreach_type(self):
        graph = CustomNamedForeachFlow._graph
        assert graph["init"].type == "foreach"
        assert graph["collect"].type == "join"
        assert graph["final"].type == "end"

    def test_custom_flow_in_funcs_out_funcs(self):
        graph = CustomNamedLinearFlow._graph
        assert graph["begin"].in_funcs == []
        assert graph["begin"].out_funcs == ["middle"]
        assert graph["finish"].in_funcs == ["middle"]
        assert graph["finish"].out_funcs == []


# ---------------------------------------------------------------------------
# Tests: output_steps / graph_structure
# ---------------------------------------------------------------------------


class TestOutputSteps:
    """Tests for FlowGraph.output_steps() with structural inference."""

    def test_standard_graph_structure(self):
        steps_info, graph_structure = StandardFlow._graph.output_steps()
        assert graph_structure == ["start", "end"]
        assert set(steps_info.keys()) == {"start", "end"}

    def test_custom_linear_graph_structure(self):
        steps_info, graph_structure = CustomNamedLinearFlow._graph.output_steps()
        assert graph_structure == ["begin", "middle", "finish"]
        assert set(steps_info.keys()) == {"begin", "middle", "finish"}

    def test_single_step_graph_structure(self):
        steps_info, graph_structure = SingleStepFlow._graph.output_steps()
        assert graph_structure == ["only"]
        assert set(steps_info.keys()) == {"only"}

    def test_branch_graph_structure(self):
        steps_info, graph_structure = CustomNamedBranchFlow._graph.output_steps()
        assert graph_structure[0] == "entry"
        assert graph_structure[-1] == "done"
        assert "merge" in graph_structure
        assert set(steps_info.keys()) == {"entry", "a", "b", "merge", "done"}

    def test_steps_info_types_match(self):
        """Steps info type should match the node_to_type mapping."""
        steps_info, _ = CustomNamedLinearFlow._graph.output_steps()
        assert steps_info["begin"]["type"] == "start"
        assert steps_info["middle"]["type"] == "linear"
        assert steps_info["finish"]["type"] == "end"

    def test_split_start_type_in_steps_info(self):
        """When start step is a split, steps_info should show split-static."""
        steps_info, _ = SplitStartFlow._graph.output_steps()
        assert steps_info["origin"]["type"] == "split-static"
        assert steps_info["terminus"]["type"] == "end"

    def test_steps_info_has_next(self):
        steps_info, _ = CustomNamedLinearFlow._graph.output_steps()
        assert steps_info["begin"]["next"] == ["middle"]
        assert steps_info["middle"]["next"] == ["finish"]
        assert steps_info["finish"]["next"] == []


# ---------------------------------------------------------------------------
# Tests: Topological sort
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def test_standard_sort(self):
        graph = StandardFlow._graph
        assert graph.sorted_nodes == ["start", "end"]

    def test_custom_linear_sort(self):
        graph = CustomNamedLinearFlow._graph
        assert graph.sorted_nodes == ["begin", "middle", "finish"]

    def test_single_step_sort(self):
        graph = SingleStepFlow._graph
        assert graph.sorted_nodes == ["only"]

    def test_branch_sort_order(self):
        """Start must come first, end must come last."""
        graph = CustomNamedBranchFlow._graph
        assert graph.sorted_nodes[0] == "entry"
        assert graph.sorted_nodes[-1] == "done"


# ---------------------------------------------------------------------------
# Tests: Lint validation
# ---------------------------------------------------------------------------


class TestLintWithStructuralInference:
    """Tests that lint checks work with structural start/end inference."""

    def test_standard_flow_passes_lint(self):
        linter.run_checks(StandardFlow._graph)

    def test_custom_linear_passes_lint(self):
        linter.run_checks(CustomNamedLinearFlow._graph)

    def test_single_step_passes_lint(self):
        linter.run_checks(SingleStepFlow._graph)

    def test_custom_branch_passes_lint(self):
        linter.run_checks(CustomNamedBranchFlow._graph)

    def test_custom_foreach_passes_lint(self):
        linter.run_checks(CustomNamedForeachFlow._graph)

    def test_split_start_passes_lint(self):
        linter.run_checks(SplitStartFlow._graph)


# ---------------------------------------------------------------------------
# Tests: Annotation mechanics
# ---------------------------------------------------------------------------


class TestAnnotationMechanics:
    """Tests that @step(start=True/end=True) annotations are stored and used correctly."""

    def test_annotation_attributes_on_plain_step(self):
        """Plain @step sets is_start_step=False and is_end_step=False."""
        graph = StandardFlow._graph
        assert graph["start"].is_start_step is False
        assert graph["start"].is_end_step is False
        assert graph["end"].is_start_step is False
        assert graph["end"].is_end_step is False

    def test_annotation_attributes_on_annotated_step(self):
        """@step(start=True) and @step(end=True) set the flags on the node."""
        graph = CustomNamedLinearFlow._graph
        assert graph["begin"].is_start_step is True
        assert graph["begin"].is_end_step is False
        assert graph["finish"].is_start_step is False
        assert graph["finish"].is_end_step is True
        # Middle step has neither
        assert graph["middle"].is_start_step is False
        assert graph["middle"].is_end_step is False

    def test_annotated_start_correct(self):
        """@step(start=True) flow has start_step matching the annotated step."""
        graph = CustomNamedLinearFlow._graph
        assert graph.start_step == "begin"

    def test_annotated_end_correct(self):
        """@step(end=True) flow has end_step matching the annotated step."""
        graph = CustomNamedLinearFlow._graph
        assert graph.end_step == "finish"

    def test_annotated_single_step(self):
        """@step(start=True, end=True) single-step flow works."""
        graph = SingleStepFlow._graph
        assert graph["only"].is_start_step is True
        assert graph["only"].is_end_step is True
        assert graph.start_step == "only"
        assert graph.end_step == "only"

    def test_mixed_annotated_start_named_end(self):
        """Annotated start + name-based end fallback: only start is annotated,
        end step is named 'end' and discovered by name fallback."""

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
        # begin has annotation, end does not
        assert graph["begin"].is_start_step is True
        assert graph["end"].is_end_step is False

    def test_backward_compat_name_based(self):
        """Flow with just 'start'/'end' names still works (no annotations)."""
        graph = StandardFlow._graph
        assert graph.start_step == "start"
        assert graph.end_step == "end"
        # No annotations set
        assert graph["start"].is_start_step is False
        assert graph["end"].is_end_step is False
