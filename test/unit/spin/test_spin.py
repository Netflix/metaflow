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

    with Runner(flow_path, environment="conda").spin(
        task.pathspec,
        artifacts_module=artifacts_path,
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

    with Runner(flow_path, environment="conda").spin(
        task.pathspec,
        artifacts_module=str(temp_artifacts_file),
    ) as spin:
        print("-" * 50)
        print(f"Running test for step: step_d with task pathspec: {task.pathspec}")
        spin_task = spin.task
        assert spin_task["my_output"].data == [-1]


def test_skip_decorators(complex_dag_run):
    print(f"Running test for skip decorator in ComplexDAGFlow: {complex_dag_run}")
    step_name = "step_m"
    task = complex_dag_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "complex_dag_flow.py")

    # Check if sklearn is available in the outer environment
    is_sklearn = True
    try:
        import sklearn
    except ImportError:
        is_sklearn = False

    if is_sklearn:
        # We verify that the sklearn version is the same as the one in the outside environment
        with Runner(flow_path, environment="conda").spin(
            task.pathspec,
            skip_decorators=True,
        ) as spin:
            print("-" * 50)
            print(
                f"Running test for step: {step_name} with task pathspec: {task.pathspec}"
            )
            spin_task = spin.task
            import sklearn

            expected_version = sklearn.__version__
            assert (
                spin_task["sklearn_version"].data == expected_version
            ), f"Expected sklearn version {expected_version} but got {spin_task['sklearn_version']}"
    else:
        # We assert that an exception is raised when trying to run the step with skip_decorators=True
        with pytest.raises(Exception):
            with Runner(flow_path, environment="conda").spin(
                task.pathspec,
                skip_decorators=True,
            ):
                pass
