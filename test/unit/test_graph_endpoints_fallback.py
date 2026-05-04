"""Tests for Run._graph_endpoints fallback and caching behavior.

The happy path (custom-named steps producing the right endpoints) is
covered by the integration tests in test/ux/core/test_basic.py
(test_custom_step_names, test_single_step_flow, test_custom_branch_flow).
This file pins the smaller, harder-to-reach behaviors:

- empty / missing metadata returns the legacy ("start", "end") fallback
- transient errors do NOT cache (so a retry can succeed)
- MetaflowNotFound (old run) DOES cache (no point retrying forever)
"""

import pytest

from metaflow.client.core import Run
from metaflow.exception import MetaflowNotFound


@pytest.fixture
def run():
    """Bare Run instance, Run.__init__ skipped to avoid metadata service I/O."""
    return Run.__new__(Run)


def _params_step(mocker, metadata):
    """Stand-in for run["_parameters"] with a controlled metadata_dict."""
    params = mocker.MagicMock()
    params.task.metadata_dict = metadata
    return params


def test_missing_metadata_falls_back_to_literals(run, mocker):
    """Empty metadata returns ('start', 'end')."""
    mocker.patch.object(Run, "__getitem__", return_value=_params_step(mocker, {}))
    assert run._graph_endpoints == ("start", "end")


def test_metaflow_not_found_caches_fallback(run, mocker):
    """MetaflowNotFound (old run, no _parameters) caches the fallback."""
    mocker.patch.object(Run, "__getitem__", side_effect=MetaflowNotFound("_parameters"))
    assert run._graph_endpoints == ("start", "end")
    assert run._cached_endpoints == ("start", "end")


def test_transient_error_not_cached(run, mocker):
    """A transient exception returns the fallback but does NOT cache it."""
    mocker.patch.object(
        Run,
        "__getitem__",
        side_effect=[
            RuntimeError("transient (e.g., metadata service down)"),
            _params_step(mocker, {"start_step": "begin", "end_step": "finish"}),
        ],
    )

    # First call: transient error, fallback returned, not cached.
    assert run._graph_endpoints == ("start", "end")
    assert not hasattr(run, "_cached_endpoints")

    # Second call: succeeds, caches.
    assert run._graph_endpoints == ("begin", "finish")
    assert run._cached_endpoints == ("begin", "finish")
