"""Tests for Run._graph_endpoints fallback behavior.

``_parameters`` task metadata is written by ``persist_constants``, which
every runtime path calls before any step runs (native runtime directly, and
orchestrators via the ``init`` command they insert). The property reads
``start_step``/``end_step`` from that metadata with a literal
``("start", "end")`` fallback for old runs.

These tests verify the lookup and that transient errors are not cached.
"""

import pytest

from metaflow.client.core import Run
from metaflow.exception import MetaflowNotFound


@pytest.fixture
def make_run(mocker):
    """Factory fixture: build a Run-like object whose ``__getitem__``
    returns a task with a controlled ``metadata_dict`` (or raises)."""

    def _factory(metadata_dict=None, raises=None):
        run = Run.__new__(Run)  # bypass __init__

        if raises is not None:

            def _getitem(key):
                raise raises

        else:
            task = mocker.MagicMock()
            task.metadata_dict = metadata_dict or {}
            params_step = mocker.MagicMock()
            params_step.task = task

            def _getitem(key):
                if key == "_parameters":
                    return params_step
                raise KeyError(key)

        run.__class__ = type(
            "FakeRun",
            (object,),
            {
                "__getitem__": lambda self, k: _getitem(k),
                "_graph_endpoints": Run._graph_endpoints,
            },
        )
        return run

    return _factory


def test_metadata_carries_endpoints(make_run):
    """Metadata has start_step/end_step, used directly."""
    run = make_run({"start_step": "begin", "end_step": "finish"})
    assert run._graph_endpoints == ("begin", "finish")


def test_missing_metadata_falls_back_to_literals(make_run):
    """Empty metadata, fall back to ('start', 'end')."""
    run = make_run({})
    assert run._graph_endpoints == ("start", "end")


def test_partial_metadata_fills_in_defaults(make_run):
    """Only one endpoint in metadata, the other defaults to its literal."""
    run = make_run({"start_step": "begin"})
    assert run._graph_endpoints == ("begin", "end")


def test_result_cached(make_run):
    """Successful lookups cache on _cached_endpoints."""
    run = make_run({"start_step": "begin", "end_step": "finish"})
    assert run._graph_endpoints == ("begin", "finish")
    assert run._cached_endpoints == ("begin", "finish")


def test_transient_error_not_cached(mocker):
    """A transient exception returns the fallback but does NOT cache it."""
    run = Run.__new__(Run)

    task = mocker.MagicMock()
    task.metadata_dict = {"start_step": "begin", "end_step": "finish"}
    params_step = mocker.MagicMock()
    params_step.task = task

    getitem = mocker.MagicMock(
        side_effect=[
            RuntimeError("transient (e.g., metadata service down)"),
            params_step,
        ]
    )

    run.__class__ = type(
        "FakeRun",
        (object,),
        {
            "__getitem__": lambda self, k: getitem(k),
            "_graph_endpoints": Run._graph_endpoints,
        },
    )

    # First call: transient error, fallback returned, not cached.
    assert run._graph_endpoints == ("start", "end")
    assert not hasattr(run, "_cached_endpoints")

    # Second call: succeeds, caches.
    assert run._graph_endpoints == ("begin", "finish")
    assert run._cached_endpoints == ("begin", "finish")


def test_metaflow_not_found_cached(make_run):
    """MetaflowNotFound (old run) caches the ('start', 'end') fallback."""
    run = make_run(raises=MetaflowNotFound("_parameters"))
    assert run._graph_endpoints == ("start", "end")
    assert run._cached_endpoints == ("start", "end")
