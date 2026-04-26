import os
import pytest
from metaflow import Runner, Flow

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def create_flow_fixture(flow_name, flow_file):
    """Factory function to create flow fixtures."""

    def flow_fixture(request):
        if request.config.getoption("--use-latest", default=False):
            flow = Flow(flow_name, _namespace_check=False)
            return flow.latest_run
        else:
            flow_path = os.path.join(FLOWS_DIR, flow_file)
            with Runner(flow_path, cwd=FLOWS_DIR).run() as running:
                return running.run

    return flow_fixture


custom_named_run = pytest.fixture(scope="session")(
    create_flow_fixture("CustomNamedFlow", "custom_named_flow.py")
)

single_step_run = pytest.fixture(scope="session")(
    create_flow_fixture("SingleStepFlow", "single_step_flow.py")
)

custom_branch_run = pytest.fixture(scope="session")(
    create_flow_fixture("CustomBranchFlow", "custom_branch_flow.py")
)

custom_named_card_run = pytest.fixture(scope="session")(
    create_flow_fixture("CustomNamedCardFlow", "custom_named_card_flow.py")
)

single_step_with_config_run = pytest.fixture(scope="session")(
    create_flow_fixture("SingleStepWithConfigFlow", "single_step_with_config_flow.py")
)

single_step_with_stacked_decos_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "SingleStepWithStackedDecosFlow",
        "single_step_with_stacked_decos_flow.py",
    )
)

single_step_with_flow_mutator_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "SingleStepWithFlowMutatorFlow",
        "single_step_with_flow_mutator_flow.py",
    )
)

single_step_bare_run = pytest.fixture(scope="session")(
    create_flow_fixture(
        "SingleStepBareFlow",
        "single_step_bare_flow.py",
    )
)
