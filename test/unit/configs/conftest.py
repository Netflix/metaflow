"""
Pytest configuration for Config tests.

Provides fixtures to run flows and access their results.
"""

import pytest
from metaflow import Runner, Flow
import os

# Get the directory containing the flows
FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def create_flow_fixture(flow_name, flow_file, run_params=None, runner_params=None):
    """
    Factory function to create flow fixtures with common logic.

    Parameters
    ----------
    flow_name : str
        Name of the flow class
    flow_file : str
        Python file containing the flow
    run_params : dict, optional
        Parameters to pass to .run() method
    runner_params : dict, optional
        Parameters to pass to Runner()
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


# Create fixtures for each test flow
config_naming_run = pytest.fixture(scope="session")(
    create_flow_fixture("ConfigNamingFlow", "config_naming_flow.py")
)

config_plain_run = pytest.fixture(scope="session")(
    create_flow_fixture("ConfigPlainFlow", "config_plain_flow.py")
)
