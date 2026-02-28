import pytest
from metaflow import Runner
import os
from spin_test_helpers import assert_artifacts, run_step, FLOWS_DIR, ARTIFACTS_DIR


@pytest.mark.parametrize(
    "flow_file,fixture_name",
    [
        ("merge_artifacts_flow.py", "merge_artifacts_run"),
        ("simple_config_flow.py", "simple_config_run"),
        ("simple_parameter_flow.py", "simple_parameter_run"),
        ("complex_dag_flow.py", "complex_dag_run"),
    ],
    ids=["merge_artifacts", "simple_config", "simple_parameter", "complex_dag"],
)
def test_simple_flows(flow_file, fixture_name, request):
    """Test simple flows that just need artifact validation."""
    run = request.getfixturevalue(fixture_name)
    print(f"Running test for {flow_file}: {run}")
    for step in run.steps():
        print("-" * 100)
        if fixture_name == "complex_dag_run":
            run_step(flow_file, run, step.id, environment="conda")
        else:
            run_step(flow_file, run, step.id)


def test_artifacts_module(complex_dag_run):
    print(f"Running test for artifacts module in ComplexDAGFlow: {complex_dag_run}")
    step_name = "step_a"
    task = complex_dag_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "complex_dag_flow.py")
    artifacts_path = os.path.join(ARTIFACTS_DIR, "complex_dag_step_a.py")

    with Runner(flow_path, cwd=FLOWS_DIR, environment="conda").spin(
        task.pathspec,
        artifacts_module=artifacts_path,
        persist=True,
    ) as spin:
        print("-" * 50)
        print(f"Running test for step: step_a with task pathspec: {task.pathspec}")
        spin_task = spin.task
        print(f"my_output: {spin_task['my_output']}")
        assert spin_task["my_output"].data == [10, 11, 12, 3]


def test_artifacts_module_join_step(
    complex_dag_run, complex_dag_step_d_artifacts, tmp_path
):
    print(f"Running test for artifacts module in ComplexDAGFlow: {complex_dag_run}")
    step_name = "step_d"
    task = complex_dag_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "complex_dag_flow.py")

    # Create a temporary artifacts file with dynamic data
    temp_artifacts_file = tmp_path / "temp_complex_dag_step_d.py"
    temp_artifacts_file.write_text(f"ARTIFACTS = {repr(complex_dag_step_d_artifacts)}")

    with Runner(flow_path, cwd=FLOWS_DIR, environment="conda").spin(
        task.pathspec,
        artifacts_module=str(temp_artifacts_file),
        persist=True,
    ) as spin:
        print("-" * 50)
        print(f"Running test for step: step_d with task pathspec: {task.pathspec}")
        spin_task = spin.task
        assert spin_task["my_output"].data == [-1]


def test_timeout_decorator_enforcement(simple_config_run):
    """Test that timeout decorator properly enforces timeout limits."""
    step_name = "start"
    task = simple_config_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_config_flow.py")

    # With decorator enabled (should timeout and raise exception)
    with pytest.raises(Exception):
        with Runner(
            flow_path, cwd=FLOWS_DIR, config_value=[("config", {"timeout": 2})]
        ).spin(
            task.pathspec,
            persist=True,
        ):
            pass


def test_skip_decorators_bypass(simple_config_run):
    """Test that skip_decorators successfully bypasses timeout decorator."""
    step_name = "start"
    task = simple_config_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_config_flow.py")

    # With skip_decorators=True (should succeed despite timeout)
    with Runner(
        flow_path, cwd=FLOWS_DIR, config_value=[("config", {"timeout": 2})]
    ).spin(
        task.pathspec,
        skip_decorators=True,
        persist=True,
    ) as spin:
        print(f"Running test for step: {step_name} with skip_decorators=True")
        # Should complete successfully even though sleep(5) > timeout(2)
        spin_task = spin.task
        assert spin_task.finished


def test_hidden_artifacts(simple_parameter_run):
    """Test simple flows that just need artifact validation."""
    step_name = "start"
    task = simple_parameter_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_parameter_flow.py")
    print(f"Running test for hidden artifacts in {flow_path}: {simple_parameter_run}")

    with Runner(flow_path, cwd=FLOWS_DIR).spin(task.pathspec, persist=True) as spin:
        spin_task = spin.task
        assert "_graph_info" in spin_task
        assert "_foreach_stack" in spin_task


def test_card_flow(simple_card_run):
    """Test a simple flow that has @card decorator."""
    step_name = "start"
    task = simple_card_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_card_flow.py")
    print(f"Running test for cards in {flow_path}: {simple_card_run}")

    with Runner(flow_path, cwd=FLOWS_DIR).spin(task.pathspec, persist=True) as spin:
        spin_task = spin.task
        from metaflow.cards import get_cards

        res = get_cards(spin_task, follow_resumed=False)
        print(res)


def test_spin_with_parameters_raises_error(simple_parameter_run):
    """Test that passing flow parameters to spin raises an error."""
    step_name = "start"
    task = simple_parameter_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_parameter_flow.py")

    with pytest.raises(Exception, match="Unknown argument"):
        with Runner(flow_path, cwd=FLOWS_DIR).spin(
            task.pathspec,
            alpha=1.0,
            persist=True,
        ):
            pass


# NOTE: This test has to be the last test because it modifies the metadata
# provider when calling inspect_spin
def test_inspect_spin_client_access(simple_parameter_run):
    """Test accessing spin artifacts using inspect_spin client directly."""
    from metaflow import inspect_spin, Task
    import tempfile

    step_name = "start"
    task = simple_parameter_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "simple_parameter_flow.py")

    with tempfile.TemporaryDirectory() as _:
        # Run spin to generate artifacts
        with Runner(flow_path, cwd=FLOWS_DIR).spin(
            task.pathspec,
            persist=True,
        ) as spin:
            spin_task = spin.task
            spin_pathspec = spin_task.pathspec
            assert spin_task["a"] is not None
            assert spin_task["b"] is not None

        assert spin_pathspec is not None

        # Set metadata provider to spin
        inspect_spin(FLOWS_DIR)
        client_task = Task(spin_pathspec, _namespace_check=False)

        # Verify task is accessible
        assert client_task is not None

        # Verify artifacts
        assert hasattr(client_task, "artifacts")

        # Verify artifact data
        assert client_task.artifacts.a.data == 10
        assert client_task.artifacts.b.data == 20
        assert client_task.artifacts.alpha.data == 0.05
