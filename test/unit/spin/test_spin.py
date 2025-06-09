import pytest
from metaflow import Runner, Run


@pytest.fixture
def complex_dag_run():
    # with Runner('complex_dag_flow.py').run() as running:
    #     yield running.run
    return Run("ComplexDAGFlow/2", _namespace_check=False)


@pytest.fixture
def merge_artifacts_run():
    # with Runner('merge_artifacts_flow.py').run() as running:
    #     yield running.run
    return Run("MergeArtifactsFlow/55", _namespace_check=False)


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


def _run_step(file_name, run, step_name):
    task = run[step_name].task
    with Runner(file_name).spin(step_name, spin_pathspec=task.pathspec) as spin:
        print("-" * 50)
        print(f"Running test for step: {step_name} with task pathspec: {task.pathspec}")
        _assert_artifacts(task, spin.task)


def test_runtime_flow(complex_dag_run):
    print(f"Running test for runtime flow: {complex_dag_run}")
    for step in complex_dag_run.steps():
        print("-" * 100)
        _run_step("runtime_dag_flow.py", complex_dag_run, step.id)


def test_merge_artifacts_flow(merge_artifacts_run):
    print(f"Running test for merge artifacts flow: {merge_artifacts_run}")
    for step in merge_artifacts_run.steps():
        print("-" * 100)
        _run_step("merge_artifacts_flow.py", merge_artifacts_run, step.id)


def test_artifacts_module(complex_dag_run):
    print(f"Running test for artifacts module in ComplexDAGFlow: {complex_dag_run}")
    step_name = "step_a"
    task = complex_dag_run[step_name].task
    with Runner("complex_dag_flow.py", environment="conda").spin(
        step_name,
        spin_pathspec=task.pathspec,
        artifacts_module="./artifacts/complex_dag_step_a.py",
    ) as spin:
        print("-" * 50)
        print(f"Running test for step: step_a with task pathspec: {task.pathspec}")
        spin_task = spin.task
        print(f"my_output: {spin_task['my_output']}")
        assert spin_task["my_output"].data == [10, 11, 12, 3]


def test_artifacts_module_join_step(complex_dag_run):
    print(f"Running test for artifacts module in ComplexDAGFlow: {complex_dag_run}")
    step_name = "step_d"
    task = complex_dag_run[step_name].task
    with Runner("complex_dag_flow.py", environment="conda").spin(
        step_name,
        spin_pathspec=task.pathspec,
        artifacts_module="./artifacts/complex_dag_step_d.py",
    ) as spin:
        print("-" * 50)
        print(f"Running test for step: step_a with task pathspec: {task.pathspec}")
        spin_task = spin.task
        print(f"my_output: {spin_task['my_output']}")
        assert spin_task["my_output"].data == [-1]
