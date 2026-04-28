import os
from importlib.util import module_from_spec, spec_from_file_location

import pytest
from metaflow import Runner, Flow

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def _load_flow_class(flow_file, flow_class_name):
    """Import a FlowSpec subclass from a file in FLOWS_DIR.

    Loads the module by file path so FLOWS_DIR is not added to sys.path.
    Used by fixtures that need to pass `flow=` to a Card constructor;
    `DefaultCardJSON` does `getattr(self.flow, step_name)` and
    `inspect.getsource(...)` to build the Task Code panel and crashes if
    `flow` is None.
    """
    spec = spec_from_file_location(flow_class_name, os.path.join(FLOWS_DIR, flow_file))
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return getattr(module, flow_class_name)


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


@pytest.fixture(scope="session")
def custom_named_card_flow():
    """Instance of CustomNamedCardFlow for tests that pass flow= to a Card."""
    return _load_flow_class("custom_named_card_flow.py", "CustomNamedCardFlow")(
        use_cli=False
    )
