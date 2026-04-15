"""
Integration tests for structural inference of start/end steps.

These tests actually execute flows with non-standard step names and verify:
- Flows run to completion
- _graph_info contains start_step/end_step
- _parameters metadata contains start_step/end_step
- Client APIs (end_task, parent_steps, child_steps) work correctly
- Single-step flows execute end-to-end
"""


class TestCustomNamedFlow:
    """Tests for a linear flow with custom step names (begin/middle/finish)."""

    def test_flow_completes(self, custom_named_run):
        assert custom_named_run.successful
        assert custom_named_run.finished

    def test_graph_info_has_endpoints(self, custom_named_run):
        """_graph_info should contain start_step and end_step fields."""
        graph_info = custom_named_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "begin"
        assert graph_info["end_step"] == "finish"

    def test_parameters_metadata_has_endpoints(self, custom_named_run):
        """_parameters task metadata should contain start_step and end_step."""
        meta = custom_named_run["_parameters"].task.metadata_dict
        assert meta.get("start_step") == "begin"
        assert meta.get("end_step") == "finish"

    def test_graph_endpoints_property(self, custom_named_run):
        """Run._graph_endpoints should return correct names."""
        start, end = custom_named_run._graph_endpoints
        assert start == "begin"
        assert end == "finish"

    def test_end_task(self, custom_named_run):
        """Run.end_task should return the 'finish' step's task."""
        end_task = custom_named_run.end_task
        assert end_task is not None
        assert end_task["x"].data == 3

    def test_steps_present(self, custom_named_run):
        """All custom-named steps should be present in the run."""
        step_names = {s.id for s in custom_named_run}
        assert step_names == {"begin", "middle", "finish"}

    def test_parent_steps(self, custom_named_run):
        """parent_steps should yield correct parents for each step."""
        begin_parents = list(custom_named_run["begin"].parent_steps)
        assert begin_parents == []

        middle_parents = [s.id for s in custom_named_run["middle"].parent_steps]
        assert middle_parents == ["begin"]

        finish_parents = [s.id for s in custom_named_run["finish"].parent_steps]
        assert finish_parents == ["middle"]

    def test_child_steps(self, custom_named_run):
        """child_steps should yield correct children for each step."""
        begin_children = [s.id for s in custom_named_run["begin"].child_steps]
        assert begin_children == ["middle"]

        middle_children = [s.id for s in custom_named_run["middle"].child_steps]
        assert middle_children == ["finish"]

        finish_children = list(custom_named_run["finish"].child_steps)
        assert finish_children == []


class TestSingleStepFlow:
    """Tests for a single-step flow where start == end."""

    def test_flow_completes(self, single_step_run):
        assert single_step_run.successful
        assert single_step_run.finished

    def test_graph_info_start_equals_end(self, single_step_run):
        """In a single-step flow, start_step and end_step should be the same."""
        graph_info = single_step_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "only"
        assert graph_info["end_step"] == "only"
        assert graph_info["start_step"] == graph_info["end_step"]

    def test_parameters_metadata(self, single_step_run):
        meta = single_step_run["_parameters"].task.metadata_dict
        assert meta.get("start_step") == "only"
        assert meta.get("end_step") == "only"

    def test_end_task(self, single_step_run):
        """end_task should return the single step's task."""
        end_task = single_step_run.end_task
        assert end_task is not None
        assert end_task["x"].data == 42

    def test_single_step_present(self, single_step_run):
        step_names = {s.id for s in single_step_run}
        assert step_names == {"only"}

    def test_parent_child_steps_empty(self, single_step_run):
        """Single step should have no parents and no children."""
        parents = list(single_step_run["only"].parent_steps)
        children = list(single_step_run["only"].child_steps)
        assert parents == []
        assert children == []


class TestCustomBranchFlow:
    """Tests for a branching flow with custom step names."""

    def test_flow_completes(self, custom_branch_run):
        assert custom_branch_run.successful
        assert custom_branch_run.finished

    def test_graph_info_endpoints(self, custom_branch_run):
        graph_info = custom_branch_run["_parameters"].task["_graph_info"].data
        assert graph_info["start_step"] == "entry"
        assert graph_info["end_step"] == "done"

    def test_end_task(self, custom_branch_run):
        end_task = custom_branch_run.end_task
        assert end_task is not None

    def test_branch_merge_data(self, custom_branch_run):
        """Data should flow through branches correctly."""
        merge_task = custom_branch_run["merge"].task
        assert sorted(merge_task["vals"].data) == ["a", "b"]

    def test_steps_present(self, custom_branch_run):
        step_names = {s.id for s in custom_branch_run}
        assert step_names == {"entry", "a", "b", "merge", "done"}

    def test_entry_has_two_children(self, custom_branch_run):
        children = [s.id for s in custom_branch_run["entry"].child_steps]
        assert sorted(children) == ["a", "b"]

    def test_merge_has_two_parents(self, custom_branch_run):
        parents = [s.id for s in custom_branch_run["merge"].parent_steps]
        assert sorted(parents) == ["a", "b"]


class TestAnnotatedFlowExecution:
    """Test that @step(start=True)/@step(end=True) works in actual execution."""

    def test_custom_named_flow_completes(self, custom_named_run):
        """custom_named_flow.py now uses annotations."""
        assert custom_named_run.successful

    def test_single_step_flow_completes(self, single_step_run):
        """single_step_flow.py now uses @step(start=True, end=True)."""
        assert single_step_run.successful

    def test_custom_branch_flow_completes(self, custom_branch_run):
        """custom_branch_flow.py now uses annotations."""
        assert custom_branch_run.successful
