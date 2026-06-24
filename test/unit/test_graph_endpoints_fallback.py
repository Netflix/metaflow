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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FALLBACK_ENDPOINTS = ("start", "end")

# ---------------------------------------------------------------------------
# Fixtures & Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def run():
    """Provide a bare Run instance, skipping Run.__init__ to avoid metadata service I/O."""
    return Run.__new__(Run)


def _mock_params_step(mocker, metadata_dict):
    """Build a mock task stand-in representing run["_parameters"] with custom metadata."""
    params = mocker.MagicMock()
    params.task.metadata_dict = metadata_dict
    return params


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_missing_metadata_falls_back_to_literals(run, mocker):
    """Verify that empty or missing metadata immediately returns the legacy fallback endpoints."""
    mocker.patch.object(Run, "__getitem__", return_value=_mock_params_step(mocker, {}))

    assert run._graph_endpoints == FALLBACK_ENDPOINTS


def test_metaflow_not_found_caches_fallback(run, mocker):
    """Verify that MetaflowNotFound (e.g., an old run lacking parameters) permanently caches the fallback."""
    mocker.patch.object(Run, "__getitem__", side_effect=MetaflowNotFound("_parameters"))

    assert run._graph_endpoints == FALLBACK_ENDPOINTS
    assert run._cached_endpoints == FALLBACK_ENDPOINTS


def test_transient_error_does_not_cache_fallback(run, mocker):
    """Verify that transient exceptions fallback safely but do NOT poison the cache for future retries."""
    mocker.patch.object(
        Run,
        "__getitem__",
        side_effect=[
            RuntimeError("transient (e.g., metadata service down)"),
            _mock_params_step(mocker, {"start_step": "begin", "end_step": "finish"}),
        ],
    )

    # First attempt: encounters a transient error, falls back, and completely avoids caching
    assert run._graph_endpoints == FALLBACK_ENDPOINTS
    assert not hasattr(run, "_cached_endpoints")

    # Second attempt: network/service recovers, successfully resolves custom steps, and caches the result
    assert run._graph_endpoints == ("begin", "finish")
    assert run._cached_endpoints == ("begin", "finish")
