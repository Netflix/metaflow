"""
Unit tests for start_step/end_step inference and @step(start/end) kwargs.

Tests graph construction only — no execution needed.
"""

import pytest
from metaflow.graph import FlowGraph


class TestStandardFlow:
    """Backward compat: steps named start/end are detected by name."""

    def setup_method(self):
        from .flows.standard_flow import StandardFlow

        self.graph = StandardFlow._graph

    def test_start_step(self):
        assert self.graph.start_step == "start"

    def test_end_step(self):
        assert self.graph.end_step == "end"

    def test_start_type(self):
        assert self.graph.nodes["start"].type == "start"

    def test_end_type(self):
        assert self.graph.nodes["end"].type == "end"

    def test_sorted_nodes(self):
        assert self.graph.sorted_nodes == ["start", "end"]


class TestCustomNamedFlow:
    """@step(start=True) / @step(end=True) with non-standard names."""

    def setup_method(self):
        from .flows.custom_named_flow import CustomNamedFlow

        self.graph = CustomNamedFlow._graph

    def test_start_step(self):
        assert self.graph.start_step == "begin"

    def test_end_step(self):
        assert self.graph.end_step == "finish"

    def test_start_type(self):
        assert self.graph.nodes["begin"].type == "start"

    def test_end_type(self):
        assert self.graph.nodes["finish"].type == "end"

    def test_middle_type(self):
        assert self.graph.nodes["middle"].type == "linear"

    def test_sorted_nodes(self):
        assert self.graph.sorted_nodes == ["begin", "middle", "finish"]

    def test_output_steps(self):
        steps_info, structure = self.graph.output_steps()
        assert "begin" in steps_info
        assert "middle" in steps_info
        assert "finish" in steps_info
        assert structure[-1] == "finish"


class TestSingleStepFlow:
    """@step(start=True, end=True) — start == end."""

    def setup_method(self):
        from .flows.single_step_flow import SingleStepFlow

        self.graph = SingleStepFlow._graph

    def test_start_equals_end(self):
        assert self.graph.start_step == "only"
        assert self.graph.end_step == "only"
        assert self.graph.start_step == self.graph.end_step

    def test_type_is_end(self):
        # Single-step: terminal node
        assert self.graph.nodes["only"].type == "end"

    def test_sorted_nodes(self):
        assert self.graph.sorted_nodes == ["only"]

    def test_output_steps(self):
        steps_info, structure = self.graph.output_steps()
        assert list(steps_info.keys()) == ["only"]
        assert structure == ["only"]


class TestAlgoSpec:
    """AlgoSpec: call method becomes a step named after the class."""

    def setup_method(self):
        from .flows.algo_spec_flow import SquareModel

        self.graph = SquareModel._graph
        self.cls = SquareModel

    def test_start_step_is_class_name(self):
        assert self.graph.start_step == "squaremodel"

    def test_start_equals_end(self):
        assert self.graph.start_step == self.graph.end_step

    def test_node_exists(self):
        assert "squaremodel" in self.graph.nodes

    def test_type_is_end(self):
        assert self.graph.nodes["squaremodel"].type == "end"

    def test_is_algo_spec(self):
        assert self.graph.is_algo_spec is True

    def test_call_is_step(self):
        assert hasattr(self.cls.call, "is_step")
        assert self.cls.call.is_step is True
        assert self.cls.call.is_start is True
        assert self.cls.call.is_end is True

    def test_output_steps(self):
        steps_info, structure = self.graph.output_steps()
        assert list(steps_info.keys()) == ["squaremodel"]
        assert structure == ["squaremodel"]

    def test_direct_callable(self):
        model = self.cls(use_cli=False)
        model.multiplier = 3.0
        model.call()
        assert model.result == 75.0  # 5^2 * 3.0


class TestConfigAlgoSpec:
    """AlgoSpec with Config and @conda_base flow decorator."""

    def setup_method(self):
        from .flows.algo_spec_config_flow import ConfigAlgoSpec

        self.cls = ConfigAlgoSpec
        self.graph = ConfigAlgoSpec._graph

    def test_step_name(self):
        assert self.graph.start_step == "configalgospec"

    def test_start_equals_end(self):
        assert self.graph.start_step == self.graph.end_step

    def test_has_config(self):
        assert hasattr(self.cls, "config")

    def test_has_parameters(self):
        params = dict(self.cls._get_parameters())
        assert "multiplier" in params

    def test_conda_base_applied(self):
        from metaflow.flowspec import FlowStateItems

        flow_decos = self.cls._flow_state.get(FlowStateItems.FLOW_DECORATORS, {})
        assert "conda_base" in flow_decos


class TestProjectAlgoSpec:
    """AlgoSpec with @project and @pypi_base flow decorators."""

    def setup_method(self):
        from .flows.algo_spec_project_flow import ProjectAlgoSpec

        self.cls = ProjectAlgoSpec
        self.graph = ProjectAlgoSpec._graph

    def test_step_name(self):
        assert self.graph.start_step == "projectalgospec"

    def test_start_equals_end(self):
        assert self.graph.start_step == self.graph.end_step

    def test_project_applied(self):
        from metaflow.flowspec import FlowStateItems

        flow_decos = self.cls._flow_state.get(FlowStateItems.FLOW_DECORATORS, {})
        assert "project" in flow_decos

    def test_pypi_base_applied(self):
        from metaflow.flowspec import FlowStateItems

        flow_decos = self.cls._flow_state.get(FlowStateItems.FLOW_DECORATORS, {})
        assert "pypi_base" in flow_decos

    def test_has_parameter(self):
        params = dict(self.cls._get_parameters())
        assert "value" in params
