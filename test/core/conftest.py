import importlib
import json
import os
import sys
from typing import Any

import pytest

from metaflow_test import FlowDefinition
from metaflow_test.formatter import FlowFormatter

# Ensure test/core/ is on sys.path so metaflow_test is importable.
_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
if _CORE_DIR not in sys.path:
    sys.path.insert(0, _CORE_DIR)


# ---------------------------------------------------------------------------
# Test discovery — owned by pytest
# ---------------------------------------------------------------------------


def _iter_graphs():
    root = os.path.join(_CORE_DIR, "graphs")
    for graphfile in os.listdir(root):
        if graphfile.endswith(".json") and not graphfile[0] == ".":
            with open(os.path.join(root, graphfile)) as f:
                yield json.load(f)


def _iter_tests():
    root = os.path.join(_CORE_DIR, "tests")
    if root not in sys.path:
        sys.path.insert(0, root)
    for testfile in os.listdir(root):
        if testfile.endswith(".py") and not testfile[0] == ".":
            mod = importlib.import_module(testfile[:-3], "metaflow_test")
            for name in dir(mod):
                obj = getattr(mod, name)
                if (
                    name not in ("MetaflowTest", "FlowDefinition")
                    and isinstance(obj, type)
                    and issubclass(obj, FlowDefinition)
                    and obj.__module__ == mod.__name__
                ):
                    yield obj()


# ---------------------------------------------------------------------------
# pytest hooks
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Checker fixture — injectable so tests can override or restrict checkers
# ---------------------------------------------------------------------------

_CORE_CHECKS = {
    "cli": {"class": "CliCheck"},
    "metadata": {"class": "MetadataCheck"},
}


@pytest.fixture(scope="session")
def core_checks() -> dict:
    """Return the checker specs run after each flow execution.

    Each entry maps a name to a dict with a "class" key: either the string
    name of a MetaflowCheck subclass ('CliCheck', 'MetadataCheck') or the
    class object itself.  Override this fixture in a conftest.py closer to
    your tests to restrict to a single checker or add a custom one.
    """
    return _CORE_CHECKS


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--core-tests",
        default=None,
        help="Comma-separated test class names to run (e.g. BasicArtifact,BasicForeach)",
    )
    parser.addoption(
        "--core-graphs",
        default=None,
        help="Comma-separated graph names to run (e.g. single-linear-step,simple-foreach)",
    )


def pytest_generate_tests(metafunc: Any) -> None:
    if "flow_triple" not in metafunc.fixturenames:
        return

    ok_tests_raw = metafunc.config.getoption("--core-tests", default=None)
    ok_graphs_raw = metafunc.config.getoption("--core-graphs", default=None)
    ok_tests = (
        {t.lower() for t in ok_tests_raw.split(",") if t} if ok_tests_raw else set()
    )
    ok_graphs = (
        {g.lower() for g in ok_graphs_raw.split(",") if g} if ok_graphs_raw else set()
    )

    # All context configuration comes from the environment (set by tox setenv).
    marker_name = os.environ.get("METAFLOW_CORE_MARKER", "local")
    executors = [
        e.strip()
        for e in os.environ.get("METAFLOW_CORE_EXECUTORS", "cli,api").split(",")
        if e.strip()
    ]
    disabled_tests = {
        t.strip()
        for t in os.environ.get("METAFLOW_CORE_DISABLED_TESTS", "").split(",")
        if t.strip()
    }
    enabled_tests = {
        t.strip()
        for t in os.environ.get("METAFLOW_CORE_ENABLED_TESTS", "").split(",")
        if t.strip()
    }
    disable_parallel = os.environ.get("METAFLOW_CORE_DISABLE_PARALLEL", "") == "1"

    mark = getattr(pytest.mark, marker_name)
    all_tests = sorted(_iter_tests(), key=lambda t: t.PRIORITY)
    all_graphs = list(_iter_graphs())

    params = []
    matched_tests = set()
    matched_graphs = set()
    for graph in all_graphs:
        graph_key = graph["name"].lower()
        if ok_graphs and graph_key not in ok_graphs:
            continue
        if disable_parallel and any(
            "num_parallel" in node for node in graph["graph"].values()
        ):
            continue

        for test in all_tests:
            test_name = test.__class__.__name__
            test_key = test_name.lower()
            if ok_tests and test_key not in ok_tests:
                continue
            if test_name in disabled_tests:
                continue
            if enabled_tests and test_name not in enabled_tests:
                continue
            if not FlowFormatter(graph, test).valid:
                continue

            matched_tests.add(test_key)
            matched_graphs.add(graph_key)
            for executor in executors:
                param_id = "%s/%s/%s/%s" % (
                    marker_name,
                    graph["name"],
                    test_name,
                    executor,
                )
                params.append(
                    pytest.param(
                        (graph, test, executor),
                        marks=[mark],
                        id=param_id,
                    )
                )

    if ok_tests:
        unknown = ok_tests - matched_tests
        if unknown:
            available = sorted(t.__class__.__name__ for t in all_tests)
            raise pytest.UsageError(
                "--core-tests: no tests matched %s.\nAvailable: %s"
                % (", ".join(sorted(unknown)), ", ".join(available))
            )
    if ok_graphs:
        unknown = ok_graphs - matched_graphs
        if unknown:
            available = sorted(g["name"] for g in all_graphs)
            raise pytest.UsageError(
                "--core-graphs: no graphs matched %s.\nAvailable: %s"
                % (", ".join(sorted(unknown)), ", ".join(available))
            )

    metafunc.parametrize("flow_triple", params)
