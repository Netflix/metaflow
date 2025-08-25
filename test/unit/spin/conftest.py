import pytest
from metaflow import Runner, Flow
import os

# Get the directory containing the flows
FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--use-latest",
        action="store_true",
        default=False,
        help="Use latest run of each flow instead of running new ones",
    )


@pytest.fixture(scope="session")
def complex_dag_run(request):
    """Run ComplexDAGFlow and return the completed run."""
    if request.config.getoption("--use-latest"):
        flow = Flow("ComplexDAGFlow", _namespace_check=False)
        return flow.latest_run
    else:
        flow_path = os.path.join(FLOWS_DIR, "complex_dag_flow.py")
        with Runner(flow_path, environment="conda", cwd=FLOWS_DIR).run() as running:
            return running.run


@pytest.fixture(scope="session")
def merge_artifacts_run(request):
    """Run MergeArtifactsFlow and return the completed run."""
    if request.config.getoption("--use-latest"):
        flow = Flow("MergeArtifactsFlow", _namespace_check=False)
        return flow.latest_run
    else:
        flow_path = os.path.join(FLOWS_DIR, "merge_artifacts_flow.py")
        with Runner(flow_path, cwd=FLOWS_DIR).run() as running:
            return running.run


@pytest.fixture(scope="session")
def simple_parameter_run(request):
    """Run SimpleParameterFlow and return the completed run."""
    if request.config.getoption("--use-latest"):
        flow = Flow("SimpleParameterFlow", _namespace_check=False)
        return flow.latest_run
    else:
        flow_path = os.path.join(FLOWS_DIR, "simple_parameter_flow.py")
        with Runner(flow_path, cwd=FLOWS_DIR).run(alpha=0.05) as running:
            return running.run


@pytest.fixture(scope="session")
def simple_card_run(request):
    """Run SimpleCardFlow and return the completed run."""
    if request.config.getoption("--use-latest"):
        flow = Flow("SimpleCardFlow", _namespace_check=False)
        return flow.latest_run
    else:
        flow_path = os.path.join(FLOWS_DIR, "simple_card_flow.py")
        with Runner(flow_path, cwd=FLOWS_DIR).run(alpha=0.05) as running:
            return running.run


@pytest.fixture(scope="session")
def simple_config_run(request):
    """Run SimpleConfigFlow and return the completed run."""
    if request.config.getoption("--use-latest"):
        flow = Flow(
            "TimeoutConfigFlow", _namespace_check=False
        )  # Note: The actual class name is TimeoutConfigFlow
        return flow.latest_run
    else:
        flow_path = os.path.join(FLOWS_DIR, "simple_config_flow.py")
        with Runner(flow_path, cwd=FLOWS_DIR).run() as running:
            return running.run


@pytest.fixture
def complex_dag_step_d_artifacts(complex_dag_run):
    """Generate dynamic artifacts for complex_dag step_d tests."""
    task = complex_dag_run["step_d"].task
    task_pathspec = next(task.parent_task_pathspecs)
    _, inp_path = task_pathspec.split("/", 1)
    return {inp_path: {"my_output": [-1]}}
