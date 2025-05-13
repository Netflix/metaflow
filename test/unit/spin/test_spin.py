import pytest
from metaflow import Runner, Run


@pytest.fixture
def complex_dag_run():
    # with Runner('complex_dag_flow.py').run() as running:
    #     yield running.run
    return Run("ComplexDAGFlow/5", _namespace_check=False)


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


def _run_step(file_name, run, step_name, is_conda=False):
    task = run[step_name].task
    if not is_conda:
        with Runner(file_name).spin(step_name, spin_pathspec=task.pathspec) as spin:
            print("-" * 50)
            print(
                f"Running test for step: {step_name} with task pathspec: {task.pathspec}"
            )
            _assert_artifacts(task, spin.task)
    else:
        with Runner(file_name, environment="conda").spin(
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
        assert spin_task["my_output"].data == [-1]


def test_skip_decorators(complex_dag_run):
    print(f"Running test for skip decorator in ComplexDAGFlow: {complex_dag_run}")
    step_name = "step_m"
    task = complex_dag_run[step_name].task
    # Check if sklearn is available in the outer environment
    # If not, this test will fail as it requires sklearn to be installed and skip_decorator
    # is set to True
    is_sklearn = True
    try:
        import sklearn
    except ImportError:
        is_sklearn = False
    if is_sklearn:
        # We verify that the sklearn version is the same as the one in the outside environment
        with Runner("complex_dag_flow.py", environment="conda").spin(
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
        with pytest.raises(Exception) as exc_info:
            with Runner("complex_dag_flow.py", environment="conda").spin(
                step_name,
                spin_pathspec=task.pathspec,
                skip_decorators=True,
            ):
                pass
