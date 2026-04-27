import os
import sys
from typing import Any

import pytest

# Ensure test/core/ is on sys.path so run_tests and metaflow_test are importable.
_CORE_DIR = os.path.dirname(os.path.abspath(__file__))
if _CORE_DIR not in sys.path:
    sys.path.insert(0, _CORE_DIR)

from contexts import ALL_CONTEXTS, CONTEXT_MARKERS


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        "--core-tests",
        default=None,
        help="Comma-separated test class names to run (e.g. BasicArtifactTest,BasicForeachTest)",
    )
    parser.addoption(
        "--core-graphs",
        default=None,
        help="Comma-separated graph names to run (e.g. single-linear-step,simple-foreach)",
    )


def pytest_generate_tests(metafunc: Any) -> None:
    if "flow_triple" not in metafunc.fixturenames:
        return

    try:
        from run_tests import iter_graphs, iter_tests
        from metaflow_test.formatter import FlowFormatter

        ok_tests_raw = metafunc.config.getoption("--core-tests", default=None)
        ok_graphs_raw = metafunc.config.getoption("--core-graphs", default=None)
        ok_tests = (
            {t.lower() for t in ok_tests_raw.split(",") if t} if ok_tests_raw else set()
        )
        ok_graphs = (
            {g.lower() for g in ok_graphs_raw.split(",") if g} if ok_graphs_raw else set()
        )

        # If METAFLOW_CORE_CONTEXT is set (e.g. by a tox setenv), only generate
        # items for that context. This keeps collection fast inside tox envs.
        active_ctx = os.environ.get("METAFLOW_CORE_CONTEXT", "")
        active_marker = os.environ.get("METAFLOW_CORE_MARKER", "")

        all_tests = sorted(iter_tests(), key=lambda t: t.PRIORITY)
        all_graphs = list(iter_graphs())

        params = []
        for context in ALL_CONTEXTS:
            if context.get("disabled", False):
                continue
            context_name = context["name"]
            marker_name = CONTEXT_MARKERS.get(context_name, "local")

            # Skip contexts that don't match the active context filter
            if active_ctx and context_name != active_ctx:
                continue
            if active_marker and marker_name != active_marker:
                continue

            mark = getattr(pytest.mark, marker_name)
            disabled_tests = set(context.get("disabled_tests", []))
            enabled_tests = set(context.get("enabled_tests", []))

            for graph in all_graphs:
                if ok_graphs and graph["name"].lower() not in ok_graphs:
                    continue
                # Skip parallel graphs for contexts that disable parallelism
                if context.get("disable_parallel", False) and any(
                    "num_parallel" in node for node in graph["graph"].values()
                ):
                    continue

                for test in all_tests:
                    test_name = test.__class__.__name__
                    if ok_tests and test_name.lower() not in ok_tests:
                        continue
                    if test_name in disabled_tests:
                        continue
                    if enabled_tests and test_name not in enabled_tests:
                        continue

                    formatter = FlowFormatter(graph, test)
                    if not formatter.valid:
                        continue

                    for executor in context["executors"]:
                        param_id = "%s/%s/%s/%s" % (
                            marker_name,
                            graph["name"],
                            test_name,
                            executor,
                        )
                        params.append(
                            pytest.param(
                                (context, graph, test, executor),
                                marks=[mark],
                                id=param_id,
                            )
                        )

        metafunc.parametrize("flow_triple", params)
    except Exception as e:
        import traceback

        print("Warning: could not generate core test combinations: %s" % e)
        traceback.print_exc()
        metafunc.parametrize("flow_triple", [])
