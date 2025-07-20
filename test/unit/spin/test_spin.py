import pytest
from metaflow import Runner
import os

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")
ARTIFACTS_DIR = os.path.join(os.path.dirname(__file__), "artifacts")


def _assert_artifacts(task, spin_task):
    spin_task_artifacts = {
        artifact.id: artifact.data for artifact in spin_task.artifacts
    }
    print(f"Spin task artifacts: {spin_task_artifacts}")
    for artifact in task.artifacts:
        assert (
            artifact.id in spin_task_artifacts
        ), f"Artifact {artifact.id} not found in spin task"
        assert (
            artifact.data == spin_task_artifacts[artifact.id]
        ), f"Expected {artifact.data} but got {spin_task_artifacts[artifact.id]} for artifact {artifact.id}"


def _run_step(flow_file, run, step_name, is_conda=False):
    task = run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, flow_file)

    if not is_conda:
        with Runner(flow_path).spin(step_name, spin_pathspec=task.pathspec) as spin:
            print("-" * 50)
            print(
                f"Running test for step: {step_name} with task pathspec: {task.pathspec}"
            )
            _assert_artifacts(task, spin.task)
    else:
        with Runner(flow_path, environment="conda").spin(
            step_name,
            spin_pathspec=task.pathspec,
        ) as spin:
            print("-" * 50)
            print(
                f"Running test for step: {step_name} with task pathspec: {task.pathspec}"
            )
            print(f"Spin task artifacts: {spin.task.artifacts}")
            _assert_artifacts(task, spin.task)


def test_complex_dag_flow(complex_dag_run):
    print(f"Running test for ComplexDAGFlow flow: {complex_dag_run}")
    for step in complex_dag_run.steps():
        print("-" * 100)
        _run_step("complex_dag_flow.py", complex_dag_run, step.id, is_conda=True)


def test_merge_artifacts_flow(merge_artifacts_run):
    print(f"Running test for merge artifacts flow: {merge_artifacts_run}")
    for step in merge_artifacts_run.steps():
        print("-" * 100)
        _run_step("merge_artifacts_flow.py", merge_artifacts_run, step.id)


def test_simple_parameter_flow(simple_parameter_run):
    print(f"Running test for SimpleParameterFlow: {simple_parameter_run}")
    for step in simple_parameter_run.steps():
        print("-" * 100)
        _run_step("simple_parameter_flow.py", simple_parameter_run, step.id)


def test_artifacts_module(complex_dag_run):
    print(f"Running test for artifacts module in ComplexDAGFlow: {complex_dag_run}")
    step_name = "step_a"
    task = complex_dag_run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, "complex_dag_flow.py")
    artifacts_path = os.path.join(ARTIFACTS_DIR, "complex_dag_step_a.py")

    with Runner(flow_path, environment="conda").spin(
        step_name,
        spin_pathspec=task.pathspec,
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
        step_name,
        spin_pathspec=task.pathspec,
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
            step_name,
            spin_pathspec=task.pathspec,
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
                step_name,
                spin_pathspec=task.pathspec,
                skip_decorators=True,
            ):
                pass
