import os
import uuid
import pytest
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

try:
    import yaml

    def _load_yaml(path):
        with open(path) as f:
            return yaml.safe_load(f) or {}

except ImportError:
    import json

    def _load_yaml(path):
        with open(path) as f:
            return json.load(f)


class ExecMode(Enum):
    RUNNER = "runner"
    DEPLOYER = "deployer"


@dataclass
class SchedulerConfig:
    scheduler_type: Optional[str]
    cluster: Optional[str]


# ---------------------------------------------------------------------------
# CLI options
# ---------------------------------------------------------------------------


def pytest_addoption(parser):
    parser.addoption(
        "--decospecs",
        action="append",
        help="Decospecs for execution. Can be specified multiple times.",
    )
    parser.addoption(
        "--cluster",
        help="Cluster / namespace for the scheduler (e.g. Argo k8s namespace).",
    )
    parser.addoption(
        "--tag",
        action="append",
        default=[str(uuid.uuid4())[:8]],
        help="Tag to use for tests. Can be specified multiple times.",
    )
    parser.addoption(
        "--exec-mode",
        choices=[mode.value for mode in ExecMode],
        help="Force a specific execution mode. If not set, tests run in both modes where supported.",
    )
    parser.addoption(
        "--scheduler-type",
        help="Scheduler type for deployer mode (step-functions, argo-workflows, …).",
    )
    parser.addoption(
        "--compute-backend",
        help="Compute backend (batch, kubernetes, fargate, …). Auto-injects into decospecs.",
    )
    parser.addoption(
        "--compute-image",
        help="Docker image for the compute backend. Injected into the auto-generated decospec.",
    )


# ---------------------------------------------------------------------------
# Config file  (ux_test_config.yaml, analogous to mf-5's ux_test_config.yaml)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def test_config():
    config_path = os.path.join(os.path.dirname(__file__), "ux_test_config.yaml")
    if os.path.exists(config_path):
        return _load_yaml(config_path)
    return {}


# ---------------------------------------------------------------------------
# Compute fixtures  (OSS equivalent of titus_image / compute_env in mf-5)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def compute_backend(request, test_config):
    """Active compute backend name (batch, kubernetes, fargate, …) or None."""
    override = request.config.getoption("--compute-backend")
    if override:
        return override
    return (test_config.get("compute") or {}).get("backend") or None


@pytest.fixture(scope="session")
def compute_image(request, test_config):
    """Docker image for the compute backend, or None."""
    override = request.config.getoption("--compute-image")
    if override:
        return override
    return (test_config.get("compute") or {}).get("image") or None


@pytest.fixture(scope="session")
def compute_env(compute_backend, compute_image):
    """
    Extra environment variables injected into every flow run.

    This is the OSS equivalent of mf-5's compute_env (which carried Titus
    image vars).  Extend this fixture in a downstream conftest.py when a
    specific backend needs runtime env vars (e.g. custom S3 endpoint for the
    devstack, Batch queue ARN, Kubernetes namespace, …).
    """
    return {}


# ---------------------------------------------------------------------------
# Decospecs  (auto-injects compute backend decorator, mirrors mf-5 behaviour)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def decospecs(request, compute_backend, compute_image):
    """
    Decorator specifications passed to Runner / Deployer.

    Mirrors mf-5: if a compute backend is configured but no explicit decospec
    for it is present in --decospecs, one is auto-injected
    (e.g. ``batch:image=<image>``).
    """
    raw = list(request.config.getoption("--decospecs") or [])

    if not compute_backend:
        return raw or None

    already_present = any(
        d == compute_backend or d.startswith(f"{compute_backend}:") for d in raw
    )

    if not already_present:
        spec = (
            f"{compute_backend}:image={compute_image}"
            if compute_image
            else compute_backend
        )
        raw.append(spec)

    def _enrich(d):
        if compute_image and d == compute_backend:
            return f"{compute_backend}:image={compute_image}"
        if compute_image and d.startswith(f"{compute_backend}:") and "image=" not in d:
            return f"{d},image={compute_image}"
        return d

    return [_enrich(d) for d in raw] or None


# ---------------------------------------------------------------------------
# Scheduler / cluster fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def scheduler_type(request, test_config):
    override = request.config.getoption("--scheduler-type")
    if override:
        return override
    return (test_config.get("scheduler") or {}).get("type") or None


@pytest.fixture(scope="session")
def cluster(request, test_config):
    override = request.config.getoption("--cluster")
    if override:
        return override
    return (test_config.get("scheduler") or {}).get("cluster") or None


@pytest.fixture(scope="session")
def scheduler_config(cluster, scheduler_type) -> SchedulerConfig:
    return SchedulerConfig(scheduler_type=scheduler_type, cluster=cluster)


# ---------------------------------------------------------------------------
# Tag
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def tag(request) -> List[str]:
    return request.config.getoption("--tag") or [str(uuid.uuid4())[:8]]


# ---------------------------------------------------------------------------
# exec_mode parametrization  (mirrors mf-5 logic)
# ---------------------------------------------------------------------------


def _has_scheduler(metafunc) -> bool:
    if metafunc.config.getoption("--scheduler-type", default=None):
        return True
    config_path = os.path.join(os.path.dirname(__file__), "ux_test_config.yaml")
    try:
        cfg = _load_yaml(config_path)
        return bool((cfg.get("scheduler") or {}).get("type"))
    except Exception:
        return False


def pytest_generate_tests(metafunc):
    """Parametrize exec_mode following the same rules as mf-5.

    - ``--exec-mode`` CLI flag forces a single mode.
    - ``scheduler_only`` / ``data_ux`` tests: deployer only (skipped when no scheduler).
    - ``basic`` / ``config`` tests: runner + deployer (deployer only when scheduler configured).
    - All other tests: deployer only (skipped when no scheduler).
    """
    if "exec_mode" not in metafunc.fixturenames:
        return

    user_exec_mode = metafunc.config.getoption("--exec-mode", default=None)
    has_scheduler = _has_scheduler(metafunc)

    scheduler_only = metafunc.definition.get_closest_marker("scheduler_only")
    data_ux_test = metafunc.definition.get_closest_marker("data_ux")
    basic_test = metafunc.definition.get_closest_marker("basic")
    config_test = metafunc.definition.get_closest_marker("config")

    if user_exec_mode:
        metafunc.parametrize("exec_mode", [user_exec_mode])
    elif scheduler_only or data_ux_test:
        modes = [ExecMode.DEPLOYER.value] if has_scheduler else []
        metafunc.parametrize("exec_mode", modes)
    elif basic_test or config_test:
        modes = [ExecMode.RUNNER.value]
        if has_scheduler:
            modes.append(ExecMode.DEPLOYER.value)
        metafunc.parametrize("exec_mode", modes)
    else:
        modes = [ExecMode.DEPLOYER.value] if has_scheduler else []
        metafunc.parametrize("exec_mode", modes)


@pytest.fixture
def exec_mode(request):
    """Current execution mode from parametrization."""
    return request.param
