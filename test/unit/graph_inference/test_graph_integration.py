"""
Integration tests for start_step/end_step — actually executes flows.

Verifies:
- Flows run to completion
- _graph_info contains start_step/end_step
- _parameters metadata contains start_step/end_step
- Client APIs (end_task, parent_steps, child_steps) work correctly
- Artifacts are persisted and readable
"""


class TestStandardFlowIntegration:
    """Standard flow with start/end names — backward compat."""

    def test_flow_completes(self, standard_run):
        assert standard_run.successful
        assert standard_run.finished

    def test_graph_info_endpoints(self, standard_run):
        graph_info = standard_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "start"
        assert graph_info["end_step"] == "end"

    def test_parameters_metadata(self, standard_run):
        meta = standard_run["_parameters"].task.metadata_dict
        assert meta.get("start_step") == "start"
        assert meta.get("end_step") == "end"

    def test_end_task(self, standard_run):
        assert standard_run.end_task is not None

    def test_steps_present(self, standard_run):
        step_names = {s.id for s in standard_run}
        assert step_names == {"start", "end"}


class TestCustomNamedFlowIntegration:
    """Flow with @step(start=True) / @step(end=True) and custom names."""

    def test_flow_completes(self, custom_named_run):
        assert custom_named_run.successful
        assert custom_named_run.finished

    def test_graph_info_endpoints(self, custom_named_run):
        graph_info = custom_named_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "begin"
        assert graph_info["end_step"] == "finish"

    def test_parameters_metadata(self, custom_named_run):
        meta = custom_named_run["_parameters"].task.metadata_dict
        assert meta.get("start_step") == "begin"
        assert meta.get("end_step") == "finish"

    def test_graph_endpoints_property(self, custom_named_run):
        start, end = custom_named_run._graph_endpoints
        assert start == "begin"
        assert end == "finish"

    def test_end_task(self, custom_named_run):
        end_task = custom_named_run.end_task
        assert end_task is not None
        assert end_task["x"].data == 3

    def test_steps_present(self, custom_named_run):
        step_names = {s.id for s in custom_named_run}
        assert step_names == {"begin", "middle", "finish"}

    def test_parent_steps(self, custom_named_run):
        begin_parents = list(custom_named_run["begin"].parent_steps)
        assert begin_parents == []

        middle_parents = [s.id for s in custom_named_run["middle"].parent_steps]
        assert middle_parents == ["begin"]

        finish_parents = [s.id for s in custom_named_run["finish"].parent_steps]
        assert finish_parents == ["middle"]

    def test_child_steps(self, custom_named_run):
        begin_children = [s.id for s in custom_named_run["begin"].child_steps]
        assert begin_children == ["middle"]

        middle_children = [s.id for s in custom_named_run["middle"].child_steps]
        assert middle_children == ["finish"]

        finish_children = list(custom_named_run["finish"].child_steps)
        assert finish_children == []


class TestSingleStepFlowIntegration:
    """Single step where start == end."""

    def test_flow_completes(self, single_step_run):
        assert single_step_run.successful
        assert single_step_run.finished

    def test_graph_info_start_equals_end(self, single_step_run):
        graph_info = single_step_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "only"
        assert graph_info["end_step"] == "only"

    def test_parameters_metadata(self, single_step_run):
        meta = single_step_run["_parameters"].task.metadata_dict
        assert meta.get("start_step") == "only"
        assert meta.get("end_step") == "only"

    def test_end_task(self, single_step_run):
        end_task = single_step_run.end_task
        assert end_task is not None
        assert end_task["x"].data == 42

    def test_single_step_present(self, single_step_run):
        step_names = {s.id for s in single_step_run}
        assert step_names == {"only"}

    def test_parent_child_empty(self, single_step_run):
        parents = list(single_step_run["only"].parent_steps)
        children = list(single_step_run["only"].child_steps)
        assert parents == []
        assert children == []


class TestAlgoSpecIntegration:
    """AlgoSpec — step named after the class."""

    def test_flow_completes(self, algo_spec_run):
        assert algo_spec_run.successful
        assert algo_spec_run.finished

    def test_graph_info_endpoints(self, algo_spec_run):
        graph_info = algo_spec_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "squaremodel"
        assert graph_info["end_step"] == "squaremodel"

    def test_parameters_metadata(self, algo_spec_run):
        meta = algo_spec_run["_parameters"].task.metadata_dict
        assert meta.get("start_step") == "squaremodel"
        assert meta.get("end_step") == "squaremodel"

    def test_end_task_data(self, algo_spec_run):
        end_task = algo_spec_run.end_task
        assert end_task is not None
        assert end_task["result"].data == 25.0  # 5^2 * 1.0 (default multiplier)

    def test_single_step(self, algo_spec_run):
        step_names = {s.id for s in algo_spec_run}
        assert step_names == {"squaremodel"}


class TestConfigAlgoSpecIntegration:
    """AlgoSpec with Config and @conda_base — full execution."""

    def test_flow_completes(self, config_algo_spec_run):
        assert config_algo_spec_run.successful
        assert config_algo_spec_run.finished

    def test_config_used_in_computation(self, config_algo_spec_run):
        """Config scale=3.0, multiplier=2.0: 25 * 2.0 * 3.0 = 150.0"""
        assert config_algo_spec_run.end_task["result"].data == 150.0

    def test_graph_endpoints(self, config_algo_spec_run):
        graph_info = config_algo_spec_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "configalgospec"
        assert graph_info["end_step"] == "configalgospec"

    def test_parameters_metadata(self, config_algo_spec_run):
        meta = config_algo_spec_run["_parameters"].task.metadata_dict
        assert meta.get("start_step") == "configalgospec"
        assert meta.get("end_step") == "configalgospec"


class TestProjectAlgoSpecIntegration:
    """AlgoSpec with @project and @pypi_base — runs in pypi env."""

    def test_flow_completes(self, project_algo_spec_run):
        assert project_algo_spec_run.successful
        assert project_algo_spec_run.finished

    def test_computation_correct(self, project_algo_spec_run):
        """value=7 (default): 7^2 = 49"""
        assert project_algo_spec_run.end_task["result"].data == 49

    def test_pypi_package_available(self, project_algo_spec_run):
        """requests was installed via @pypi_base and used in call()."""
        assert project_algo_spec_run.end_task["requests_version"].data == "2.31.0"

    def test_graph_endpoints(self, project_algo_spec_run):
        graph_info = project_algo_spec_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "projectalgospec"
        assert graph_info["end_step"] == "projectalgospec"

    def test_single_step(self, project_algo_spec_run):
        step_names = {s.id for s in project_algo_spec_run}
        assert step_names == {"projectalgospec"}
