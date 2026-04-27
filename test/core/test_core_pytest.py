"""
Core integration tests for Metaflow.

Each pytest item corresponds to one (context, graph, test, executor) combination.
The flow_triple fixture (parametrized in conftest.py) provides the combination;
this module runs it directly via run_test() without a subprocess wrapper.

Usage:
    tox -e core-local                         # local backend via tox
    pytest test/core/ -m local                # local backend, all tests
    pytest test/core/ -m local -n auto        # parallel with xdist
    pytest test/core/ -m local \\
        --core-tests BasicArtifactTest \\
        --core-graphs single-linear-step      # targeted run
"""

import os
import sys
from typing import Tuple

import pytest

_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
if _CORE_DIR not in sys.path:
    sys.path.insert(0, _CORE_DIR)

from contexts import CHECKS, CONTEXT_MARKERS
from run_tests import run_test
from metaflow_test.formatter import FlowFormatter


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


def test_flow_triple(flow_triple: Tuple) -> None:
    """Run one (context, graph, test, executor) combination.

    The flow_triple fixture is parametrized by conftest.pytest_generate_tests,
    which generates one item per valid combination. Each item runs as an
    independent pytest test, enabling parallel execution via pytest-xdist
    and per-test timeout/failure isolation.
    """
    context, graph, test, executor = flow_triple

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
            checks=CHECKS,
            env_base=env_base,
            executor=executor,
        )

    if ret != 0:
        marker = CONTEXT_MARKERS.get(context["name"], context["name"])
        pytest.fail(
            "Core test failed: %s/%s/%s/%s\n  flow path: %s"
            % (marker, graph["name"], test.__class__.__name__, executor, path)
        )
