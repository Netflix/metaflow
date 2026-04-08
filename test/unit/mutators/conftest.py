"""Fixtures for mutator system tests."""

import os
import pytest
from metaflow import Runner

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


def create_flow_fixture(flow_file):
    """Factory for session-scoped flow run fixtures."""

    def fixture_func():
        flow_path = os.path.join(FLOWS_DIR, flow_file)
        with Runner(flow_path, cwd=FLOWS_DIR).run() as running:
            return running.run

    return fixture_func


dual_inherit_run = pytest.fixture(scope="session")(
    create_flow_fixture("dual_inherit_flow.py")
)

add_decorator_return_run = pytest.fixture(scope="session")(
    create_flow_fixture("add_decorator_return_flow.py")
)

dynamic_flow_mutator_run = pytest.fixture(scope="session")(
    create_flow_fixture("dynamic_flow_mutator_flow.py")
)

string_flow_mutator_run = pytest.fixture(scope="session")(
    create_flow_fixture("string_flow_mutator_flow.py")
)

string_step_mutator_run = pytest.fixture(scope="session")(
    create_flow_fixture("string_step_mutator_flow.py")
)
