"""
OSS Metaflow UX test configuration.

Parametrizes tests over the list of backends defined in ux_test_config.yaml.
Each enabled backend produces its own (scheduler_config, decospecs) combination,
giving the full cross-product of orchestrator x compute backend.
"""

import os
import uuid
import pytest
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from omegaconf import OmegaConf


class ExecMode(Enum):
    RUNNER = "runner"
    DEPLOYER = "deployer"


@dataclass
class SchedulerConfig:
    scheduler_type: Optional[str]
    cluster: Optional[str]

    def get(self, key, default=None):
        return getattr(self, key, default)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _load_config(rootdir=None):
    candidates = []
    if rootdir:
        candidates.append(os.path.join(str(rootdir), "ux_test_config.yaml"))
    candidates.append(os.path.join(os.path.dirname(__file__), "ux_test_config.yaml"))
    for path in candidates:
        if os.path.exists(path):
            return OmegaConf.load(path)
    return OmegaConf.create({})


def _enabled_backends(cfg):
    """Return the list of enabled backend dicts from config."""
    backends = cfg.get("backends", None)
    if not backends:
        # Legacy single-backend format
        sched = cfg.get("scheduler", {}) or {}
        compute = cfg.get("compute", {}) or {}
        backend_name = compute.get("backend") or None
        image = compute.get("image") or None
        decospec = None
        if backend_name and image:
            decospec = "%s:image=%s" % (backend_name, image)
        elif backend_name:
            decospec = backend_name
        return [
            {
                "name": "default",
                "scheduler_type": sched.get("type") or None,
                "cluster": sched.get("cluster") or None,
                "decospec": decospec,
                "enabled": True,
            }
        ]

    result = []
    for b in backends:
        b = OmegaConf.to_container(b, resolve=True)
        if b.get("enabled", True):
            result.append(b)
    return result


# ---------------------------------------------------------------------------
# CLI options
# ---------------------------------------------------------------------------


def pytest_addoption(parser):
    parser.addoption(
        "--decospecs",
        action="append",
        help="Decospecs for execution. Overrides config file.",
    )
    parser.addoption(
        "--cluster",
        help="Cluster / namespace for the scheduler. Overrides config file.",
    )
    parser.addoption(
        "--tag",
        action="append",
        default=None,
        help="Tag to use for tests.",
    )
    parser.addoption(
        "--exec-mode",
        choices=[m.value for m in ExecMode],
        help="Force a specific execution mode.",
    )
    parser.addoption(
        "--scheduler-type",
        help="Scheduler type for deployer mode. Overrides config file.",
    )
    parser.addoption(
        "--only-backend",
        help=(
            "Run tests only for the named backend from ux_test_config.yaml "
            "(e.g. 'local', 'argo-kubernetes'). "
            "Useful in CI matrix jobs to isolate one backend per runner."
        ),
    )


# ---------------------------------------------------------------------------
# Session-scoped fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def test_config(request):
    rootdir = getattr(request.config, "rootdir", None)
    return _load_config(rootdir)


@pytest.fixture(scope="session")
def tag(request) -> List[str]:
    t = request.config.getoption("--tag")
    return t if t else [str(uuid.uuid4())[:8]]


@pytest.fixture(scope="session")
def compute_env():
    """
    Extra environment variables for flow execution.
    Override in a local conftest.py for custom endpoints (e.g. MinIO).
    """
    return {}


# ---------------------------------------------------------------------------
# Backend parametrization
#
# Each test is parametrized over (exec_mode, backend) pairs.
# backend is a dict: {name, scheduler_type, cluster, decospec}
# ---------------------------------------------------------------------------


def _resolve_backends(
    config, sched_override=None, cluster_override=None, decospec_override=None
):
    backends = _enabled_backends(config)
    if sched_override:
        return [
            {
                "name": sched_override,
                "scheduler_type": sched_override,
                "cluster": cluster_override,
                "decospec": decospec_override,
                "enabled": True,
            }
        ]
    if decospec_override:
        backends = [dict(b, decospec=decospec_override) for b in backends]
    return backends


def pytest_generate_tests(metafunc):
    """
    Parametrize (exec_mode, backend) for each test.

    Rules (mirrors mf-5 conftest):
    - --exec-mode forces a single exec_mode.
    - scheduler_only / data_ux: deployer only (skipped for backends without scheduler).
    - basic / config: runner + deployer (deployer only when backend has scheduler).
    - Other tests: deployer only.
    """
    cfg_path = os.path.join(os.path.dirname(__file__), "ux_test_config.yaml")
    try:
        cfg = OmegaConf.load(cfg_path)
    except Exception:
        cfg = OmegaConf.create({})

    sched_override = metafunc.config.getoption("--scheduler-type", default=None)
    cluster_override = metafunc.config.getoption("--cluster", default=None)
    raw_decospecs = list(metafunc.config.getoption("--decospecs") or [])
    decospec_override = raw_decospecs[0] if raw_decospecs else None

    backends = _resolve_backends(
        cfg, sched_override, cluster_override, decospec_override
    )

    only_backend = metafunc.config.getoption("--only-backend", default=None)
    if only_backend:
        backends = [b for b in backends if b.get("name") == only_backend]

    user_exec_mode = metafunc.config.getoption("--exec-mode", default=None)
    scheduler_only = metafunc.definition.get_closest_marker("scheduler_only")
    data_ux = metafunc.definition.get_closest_marker("data_ux")
    basic = metafunc.definition.get_closest_marker("basic")
    config_mark = metafunc.definition.get_closest_marker("config")

    needs_exec = "exec_mode" in metafunc.fixturenames
    needs_backend = "backend" in metafunc.fixturenames

    if not needs_exec and not needs_backend:
        return

    params = []
    ids = []

    for b in backends:
        has_scheduler = bool(b.get("scheduler_type"))

        if user_exec_mode:
            modes = [user_exec_mode]
        elif scheduler_only or data_ux:
            modes = [ExecMode.DEPLOYER.value] if has_scheduler else []
        elif basic or config_mark:
            modes = [ExecMode.RUNNER.value]
            if has_scheduler:
                modes.append(ExecMode.DEPLOYER.value)
        else:
            modes = [ExecMode.DEPLOYER.value] if has_scheduler else []

        for mode in modes:
            if needs_exec and needs_backend:
                params.append((mode, b))
            elif needs_exec:
                params.append(mode)
            else:
                params.append(b)
            ids.append("%s-%s" % (b["name"], mode))

    if needs_exec and needs_backend:
        metafunc.parametrize(["exec_mode", "backend"], params, ids=ids)
    elif needs_exec:
        metafunc.parametrize("exec_mode", params, ids=ids)
    else:
        metafunc.parametrize("backend", params, ids=ids)


@pytest.fixture
def exec_mode(request):
    return request.param


@pytest.fixture
def backend(request):
    return request.param


@pytest.fixture
def scheduler_config(backend) -> SchedulerConfig:
    return SchedulerConfig(
        scheduler_type=backend.get("scheduler_type"),
        cluster=backend.get("cluster"),
    )


@pytest.fixture
def backend_name(backend) -> str:
    return backend.get("name", "unknown")


@pytest.fixture
def decospecs(request, backend):
    raw = list(request.config.getoption("--decospecs") or [])
    if raw:
        return raw
    ds = backend.get("decospec")
    return [ds] if ds else None
