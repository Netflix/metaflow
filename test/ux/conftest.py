import os
import uuid
import pytest
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class ExecMode(Enum):
    """Execution modes for tests"""

    RUNNER = "runner"
    DEPLOYER = "deployer"


@dataclass
class SchedulerConfig:
    cluster: Optional[str]
    scheduler_type: Optional[str]


def pytest_addoption(parser):
    parser.addoption(
        "--decospecs",
        action="append",
        help="Decospecs for execution (e.g. 'batch', 'kubernetes'). Can be specified multiple times.",
    )
    parser.addoption(
        "--tag",
        action="append",
        default=None,
        help="Tag to use for tests. Defaults to a random UUID.",
    )
    parser.addoption(
        "--exec-mode",
        choices=[mode.value for mode in ExecMode],
        default=None,
        help="Execution mode. Defaults to 'runner'. Use 'deployer' with --scheduler-type.",
    )
    parser.addoption("--cluster", default=None, help="Cluster for scheduler deployment")
    parser.addoption(
        "--scheduler-type",
        default=None,
        help="Scheduler type for deployer mode (e.g. 'step-functions', 'argo-workflows')",
    )


def pytest_generate_tests(metafunc):
    """Generate test parameters based on exec_mode requirements."""
    if "exec_mode" not in metafunc.fixturenames:
        return

    user_exec_mode = metafunc.config.getoption("--exec-mode")
    scheduler_type = metafunc.config.getoption("--scheduler-type")

    scheduler_only = metafunc.definition.get_closest_marker("scheduler_only")
    basic_test = metafunc.definition.get_closest_marker("basic")
    config_test = metafunc.definition.get_closest_marker("config")

    if user_exec_mode:
        metafunc.parametrize("exec_mode", [user_exec_mode])
    elif scheduler_only:
        if scheduler_type:
            metafunc.parametrize("exec_mode", [ExecMode.DEPLOYER.value])
        else:
            # Skip scheduler-only tests if no scheduler is configured
            metafunc.parametrize("exec_mode", [])
    elif basic_test or config_test:
        modes = [ExecMode.RUNNER.value]
        if scheduler_type:
            modes.append(ExecMode.DEPLOYER.value)
        metafunc.parametrize("exec_mode", modes)
    else:
        modes = [ExecMode.RUNNER.value]
        if scheduler_type:
            modes.append(ExecMode.DEPLOYER.value)
        metafunc.parametrize("exec_mode", modes)


@pytest.fixture
def exec_mode(request):
    """Get the current exec_mode from parametrization."""
    return request.param


@pytest.fixture(scope="session")
def decospecs(request):
    return request.config.getoption("--decospecs")


@pytest.fixture(scope="session")
def tag(request):
    tags = request.config.getoption("--tag")
    if not tags:
        return [str(uuid.uuid4())[:8]]
    return tags


@pytest.fixture(scope="session")
def compute_env():
    """Environment variables for compute. Override via --decospecs for cloud compute."""
    return {}


@pytest.fixture(scope="session")
def scheduler_config(request) -> SchedulerConfig:
    return SchedulerConfig(
        cluster=request.config.getoption("--cluster"),
        scheduler_type=request.config.getoption("--scheduler-type"),
    )
