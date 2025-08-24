import os
from metaflow import Runner

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")
ARTIFACTS_DIR = os.path.join(os.path.dirname(__file__), "artifacts")


def assert_artifacts(task, spin_task):
    """Assert that artifacts match between original task and spin task."""
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


def run_step(flow_file, run, step_name, **tl_kwargs):
    """Run a step and assert artifacts match."""
    task = run[step_name].task
    flow_path = os.path.join(FLOWS_DIR, flow_file)
    print(f"FLOWS_DIR: {FLOWS_DIR}")

    with Runner(flow_path, cwd=FLOWS_DIR, **tl_kwargs).spin(task.pathspec) as spin:
        print("-" * 50)
        print(f"Running test for step: {step_name} with task pathspec: {task.pathspec}")
        assert_artifacts(task, spin.task)
