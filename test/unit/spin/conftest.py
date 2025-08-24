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


def create_flow_fixture(flow_name, flow_file, run_params=None, runner_params=None):
    """Factory function to create flow fixtures with common logic.

    Args:
        flow_name: Name of the flow class
        flow_file: Python file containing the flow
        run_params: Parameters to pass to .run() method
        runner_params: Parameters to pass to Runner() constructor
    """

    def flow_fixture(request):
        if request.config.getoption("--use-latest"):
            flow = Flow(flow_name, _namespace_check=False)
            return flow.latest_run
        else:
            flow_path = os.path.join(FLOWS_DIR, flow_file)
            runner_params_dict = runner_params or {}
            runner_params_dict["cwd"] = FLOWS_DIR  # Always set cwd to FLOWS_DIR
            run_params_dict = run_params or {}

            with Runner(flow_path, **runner_params_dict).run(
                **run_params_dict
            ) as running:
                return running.run

    return flow_fixture


# Create all the flow fixtures using the factory
complex_dag_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "ComplexDAGFlow", "complex_dag_flow.py", runner_params={"environment": "conda"}
    )
)

merge_artifacts_run = pytest.fixture(scope="session")(
    create_flow_fixture("MergeArtifactsFlow", "merge_artifacts_flow.py")
)

simple_parameter_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "SimpleParameterFlow", "simple_parameter_flow.py", run_params={"alpha": 0.05}
    )
)

simple_card_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "SimpleCardFlow", "simple_card_flow.py", run_params={"alpha": 0.05}
    )
)

simple_config_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "TimeoutConfigFlow",
        "simple_config_flow.py",
    )
)


@pytest.fixture
def complex_dag_step_d_artifacts(complex_dag_run):
    """Generate dynamic artifacts for complex_dag step_d tests."""
    task = complex_dag_run["step_d"].task
    task_pathspec = next(task.parent_task_pathspecs)
    _, inp_path = task_pathspec.split("/", 1)
    return {inp_path: {"my_output": [-1]}}
