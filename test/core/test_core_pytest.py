"""
Core integration tests for Metaflow.

Each pytest item corresponds to one (graph, test, executor) combination.
All context configuration (Metaflow env vars, top_options, disabled tests, etc.)
comes from the environment — set by the tox env's setenv block. There is no
Python context file; the tox env IS the context.

Usage:
    tox -e core-local                         # local backend via tox
    tox -e core-gcs                           # gcs marker via tox
    pytest test/core/ -m local                # local backend, all tests
    pytest test/core/ -m local -n auto        # parallel with xdist
    pytest test/core/ -m local \\
        --core-tests BasicArtifactTest \\
        --core-graphs single-linear-step      # targeted run
"""

import os
import shlex
import sys
from typing import Tuple

import pytest

_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
if _CORE_DIR not in sys.path:
    sys.path.insert(0, _CORE_DIR)

from run_tests import run_test
from metaflow_test.formatter import FlowFormatter

_SASHIMI = "刺身 means sashimi"

_DEFAULT_RUN_OPTIONS = [
    "--max-workers=50",
    "--max-num-splits=10000",
    "--tag=%s" % _SASHIMI,
    "--tag=multiple tags should be ok",
]

_ALL_CHECKS = {
    "python3-cli": {"python": "python3", "class": "CliCheck"},
    "python3-metadata": {"python": "python3", "class": "MetadataCheck"},
}


class _WithDir:
    """Temporarily change the working directory, restoring it on exit.

    run_test() captures os.getcwd() to locate metaflow_test/ and tests/,
    so it must be called with cwd = test/core/.
    """

    def __init__(self, new_dir: str) -> None:
        self._old = os.getcwd()
        self._new = new_dir

    def __enter__(self) -> str:
        os.chdir(self._new)
        return self._new

    def __exit__(self, *_) -> None:
        os.chdir(self._old)


def _context_from_env() -> dict:
    """Build a run_test()-compatible context dict from tox setenv vars."""
    top_options = shlex.split(os.environ.get("METAFLOW_CORE_TOP_OPTIONS", ""))
    check_names = [
        c
        for c in os.environ.get(
            "METAFLOW_CORE_CHECKS", "python3-cli,python3-metadata"
        ).split(",")
        if c
    ]
    ctx = {
        "name": os.environ.get("METAFLOW_CORE_MARKER", "local"),
        "python": "python3",
        "top_options": top_options,
        "run_options": _DEFAULT_RUN_OPTIONS,
        "checks": check_names,
        # env is intentionally empty: all Metaflow config vars are already in
        # os.environ via tox setenv and will be inherited by run_test().
        "env": {},
    }
    scheduler = os.environ.get("METAFLOW_CORE_SCHEDULER", "")
    if scheduler:
        ctx["scheduler"] = scheduler
        ctx["scheduler_timeout"] = int(
            os.environ.get("METAFLOW_CORE_SCHEDULER_TIMEOUT", "600")
        )
    return ctx


def test_flow_triple(flow_triple: Tuple) -> None:
    """Run one (graph, test, executor) combination.

    The flow_triple fixture is parametrized by conftest.pytest_generate_tests,
    which generates one item per valid combination. Each item runs as an
    independent pytest test, enabling parallel execution via pytest-xdist
    and per-test timeout/failure isolation.
    """
    graph, test, executor = flow_triple
    context = _context_from_env()

    # METAFLOW_USER must be set before metaflow imports so that the cached
    # USER value is non-root (required for the api executor on root hosts).
    env_base = {
        "METAFLOW_CLICK_API_PROCESS_CONFIG": "0",
        "METAFLOW_TEST_PRINT_FLOW": "1",
        "METAFLOW_USER": os.environ.get("METAFLOW_USER", "tester"),
    }

    formatter = FlowFormatter(graph, test)

    # run_test() uses os.getcwd() to locate metaflow_test/ and tests/.
    with _WithDir(_CORE_DIR):
        ret, path = run_test(
            formatter=formatter,
            context=context,
            debug=False,
            checks=_ALL_CHECKS,
            env_base=env_base,
            executor=executor,
        )

    if ret != 0:
        marker = os.environ.get("METAFLOW_CORE_MARKER", "local")
        pytest.fail(
            "Core test failed: %s/%s/%s/%s\n  flow path: %s"
            % (marker, graph["name"], test.__class__.__name__, executor, path)
        )
