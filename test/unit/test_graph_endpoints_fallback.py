"""Tests for Run._graph_endpoints fallback behavior.

``_parameters`` task metadata is written by ``persist_constants``, which
every runtime path calls before any step runs (native runtime directly, and
orchestrators via the ``init`` command they insert). The property reads
``start_step``/``end_step`` from that metadata with a literal
``("start", "end")`` fallback for old runs.

These tests verify the lookup and that transient errors are not cached.
"""

from unittest.mock import MagicMock

from metaflow.client.core import Run
from metaflow.exception import MetaflowNotFound


def _make_run(metadata_dict):
    """Build a Run-like object with a controlled ``_parameters`` metadata dict."""
    run = Run.__new__(Run)  # bypass __init__

    task = MagicMock()
    task.metadata_dict = metadata_dict
    params_step = MagicMock()
    params_step.task = task

    def _getitem(key):
        if key == "_parameters":
            return params_step
        raise KeyError(key)

    run.__class__ = type(
        "FakeRun", (object,), {"__getitem__": lambda self, k: _getitem(k)}
    )
    run.__class__._graph_endpoints = Run._graph_endpoints
    return run


def test_metadata_carries_endpoints():
    """Metadata has start_step/end_step -- used directly."""
    run = _make_run({"start_step": "begin", "end_step": "finish"})
    assert run._graph_endpoints == ("begin", "finish")


def test_missing_metadata_falls_back_to_literals():
    """Empty metadata -- fall back to ("start", "end")."""
    run = _make_run({})
    assert run._graph_endpoints == ("start", "end")


def test_partial_metadata_fills_in_defaults():
    """Only one endpoint in metadata -- the other defaults to its literal."""
    run = _make_run({"start_step": "begin"})
    assert run._graph_endpoints == ("begin", "end")


def test_result_cached():
    """Successful lookups cache on _cached_endpoints."""
    run = _make_run({"start_step": "begin", "end_step": "finish"})
    assert run._graph_endpoints == ("begin", "finish")
    assert run._cached_endpoints == ("begin", "finish")


def test_transient_error_not_cached():
    """A transient exception returns the fallback but does NOT cache it."""
    run = Run.__new__(Run)
    call_count = {"n": 0}

    def _getitem(key):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("transient (e.g., metadata service down)")
        params_step = MagicMock()
        task = MagicMock()
        task.metadata_dict = {"start_step": "begin", "end_step": "finish"}
        params_step.task = task
        return params_step

    run.__class__ = type(
        "FakeRun2", (object,), {"__getitem__": lambda self, k: _getitem(k)}
    )
    run.__class__._graph_endpoints = Run._graph_endpoints

    # First call: transient error -> fallback, not cached.
    assert run._graph_endpoints == ("start", "end")
    assert not hasattr(run, "_cached_endpoints")

    # Second call: succeeds, caches.
    assert run._graph_endpoints == ("begin", "finish")
    assert run._cached_endpoints == ("begin", "finish")


def test_metaflow_not_found_cached():
    """MetaflowNotFound (old run) caches the ("start", "end") fallback."""
    run = Run.__new__(Run)

    def _getitem(key):
        raise MetaflowNotFound(key)

    run.__class__ = type(
        "FakeRun3", (object,), {"__getitem__": lambda self, k: _getitem(k)}
    )
    run.__class__._graph_endpoints = Run._graph_endpoints

    assert run._graph_endpoints == ("start", "end")
    # Cached -- a second call won't re-raise.
    assert run._cached_endpoints == ("start", "end")
