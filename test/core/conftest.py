import json
import os
import sys
from itertools import product
from typing import Any, Iterator, List

import pytest

# Short marker name for each context. Used for -m filtering (e.g. pytest -m local).
_CONTEXT_MARKERS = {
    "python3-all-local": "local",
    "python3-all-local-azure-storage": "azure",
    "python3-all-local-gcs": "gcs",
    "python3-batch": "batch",
    "python3-k8s": "k8s",
    "python3-argo-workflows": "argo",
    "python3-sfn": "sfn",
}


def pytest_configure(config: Any) -> None:
    for mark, description in [
        ("local", "local datastore/metadata context"),
        ("azure", "Azure blob storage context"),
        ("gcs", "Google Cloud Storage context"),
        ("batch", "AWS Batch context"),
        ("k8s", "Kubernetes context"),
        ("argo", "Argo Workflows context"),
        ("sfn", "AWS Step Functions context"),
    ]:
        config.addinivalue_line("markers", "%s: %s" % (mark, description))


def pytest_generate_tests(metafunc: Any) -> None:
    if "core_test_params" not in metafunc.fixturenames:
        return
    try:
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

        all_tests = sorted(iter_tests(), key=lambda t: t.PRIORITY)
        all_graphs = list(iter_graphs())

        # Group tests into batches of 10 to keep each pytest item manageable.
        batch_size = 10
        test_batches = [
            all_tests[i : i + batch_size] for i in range(0, len(all_tests), batch_size)
        ]

        params = []
        for context_name, graph, batch in product(
            enabled_contexts, all_graphs, test_batches
        ):
            valid = [
                t.__class__.__name__ for t in batch if FlowFormatter(graph, t).valid
            ]
            if not valid:
                continue

            marker_name = _CONTEXT_MARKERS.get(context_name, "local")
            short_ctx = marker_name
            # Build a readable ID: context/graph/FirstTest[+N more]
            if len(valid) == 1:
                test_label = valid[0]
            else:
                test_label = "%s+%d" % (valid[0], len(valid) - 1)
            param_id = "%s/%s/%s" % (short_ctx, graph["name"], test_label)

            params.append(
                pytest.param(
                    (context_name, graph["name"], valid),
                    marks=[getattr(pytest.mark, marker_name)],
                    id=param_id,
                )
            )

        metafunc.parametrize("core_test_params", params)
    except Exception as e:
        print("Warning: could not generate core test combinations: %s" % e)
        metafunc.parametrize("core_test_params", [])


@pytest.fixture
def masked_cpu_count() -> int:
    return len(getattr(os, "sched_getaffinity", lambda _: [])(0)) or os.cpu_count()
