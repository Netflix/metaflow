import os
import pytest
from metaflow import Runner, Flow

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def create_flow_fixture(flow_name, flow_file, **runner_kwargs):
    """Factory function to create flow fixtures that run via Runner."""

    def flow_fixture(request):
        flow_path = os.path.join(FLOWS_DIR, flow_file)
        with Runner(flow_path, cwd=FLOWS_DIR, **runner_kwargs).run() as running:
            return running.run

    return flow_fixture


custom_named_run = pytest.fixture(scope="session")(
    create_flow_fixture("CustomNamedFlow", "custom_named_flow.py")
)

single_step_run = pytest.fixture(scope="session")(
    create_flow_fixture("SingleStepFlow", "single_step_flow.py")
)

standard_run = pytest.fixture(scope="session")(
    create_flow_fixture("StandardFlow", "standard_flow.py")
)

algo_spec_run = pytest.fixture(scope="session")(
    create_flow_fixture("SquareModel", "algo_spec_flow.py")
)

config_algo_spec_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "ConfigAlgoSpec",
        "algo_spec_config_flow.py",
        environment="conda",
    )
)

project_algo_spec_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "ProjectAlgoSpec",
        "algo_spec_project_flow.py",
        environment="conda",
    )
)
