import pytest
from metaflow import Runner
import os

# Get the directory containing the flows
FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


@pytest.fixture(scope="session")
def complex_dag_run():
    """Run ComplexDAGFlow and return the completed run."""
    flow_path = os.path.join(FLOWS_DIR, "complex_dag_flow.py")
    with Runner(flow_path, environment="conda").run() as running:
        return running.run


@pytest.fixture(scope="session")
def merge_artifacts_run():
    """Run MergeArtifactsFlow and return the completed run."""
    flow_path = os.path.join(FLOWS_DIR, "merge_artifacts_flow.py")
    with Runner(flow_path).run() as running:
        return running.run


@pytest.fixture(scope="session")
def simple_parameter_run():
    """Run SimpleParameterFlow and return the completed run."""
    flow_path = os.path.join(FLOWS_DIR, "simple_parameter_flow.py")
    with Runner(flow_path).run(alpha=0.05) as running:
        return running.run


@pytest.fixture
def complex_dag_step_d_artifacts(complex_dag_run):
    """Generate dynamic artifacts for complex_dag step_d tests."""
    task = complex_dag_run["step_d"].task
    task_pathspec = next(task.parent_task_pathspecs)
    _, inp_path = task_pathspec.split("/", 1)
    return {inp_path: {"my_output": [-1]}}
