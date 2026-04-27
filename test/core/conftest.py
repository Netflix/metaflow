import json
import os
from itertools import product
from typing import Any, Iterator, List

import pytest


def _split_into_batches(lst: List[Any], batch_size: int) -> Iterator[List[Any]]:
    # Skip card tests — they need separate infrastructure
    non_card = [t for t in lst if "Card" not in t.__class__.__name__]
    for i in range(0, len(non_card), batch_size):
        yield non_card[i : i + batch_size]


def pytest_generate_tests(metafunc: Any) -> None:
    if "core_test_params" not in metafunc.fixturenames:
        return
    try:
        import sys

        core_dir = os.path.dirname(__file__)
        if core_dir not in sys.path:
            sys.path.insert(0, core_dir)

        with open(os.path.join(core_dir, "contexts.json")) as f:
            contexts = json.load(f)

        enabled_contexts = [
            c["name"]
            for c in contexts["contexts"]
            if not c["name"].startswith("dev") and not c.get("disabled", False)
        ]

        from run_tests import iter_graphs, iter_tests
        from metaflow_test.formatter import FlowFormatter

        test_batches = list(_split_into_batches(list(iter_tests()), batch_size=10))
        all_graphs = list(iter_graphs())

        params = []
        for context, graph, batch in product(
            enabled_contexts, all_graphs, test_batches
        ):
            valid = [
                t.__class__.__name__ for t in batch if FlowFormatter(graph, t).valid
            ]
            if valid:
                params.append((context, graph["name"], valid))

        metafunc.parametrize("core_test_params", params)
    except Exception as e:
        print("Warning: could not generate core test combinations: %s" % e)
        metafunc.parametrize("core_test_params", [])


@pytest.fixture
def masked_cpu_count() -> int:
    return len(getattr(os, "sched_getaffinity", lambda _: [])(0)) or os.cpu_count()
