"""Tests for _FlowState MutableMapping compliance.

_FlowState and FlowStateItems only depend on stdlib (MutableMapping, Enum),
but importing metaflow.flowspec triggers transitive imports that may fail on
some platforms (e.g. fcntl on Windows).  We attempt a direct import and skip
gracefully when the platform cannot satisfy the dependency chain.
"""

import pytest

try:
    from metaflow.flowspec import _FlowState, FlowStateItems
except Exception as _exc:
    pytest.skip(
        f"Cannot import metaflow.flowspec on this platform: {_exc}",
        allow_module_level=True,
    )


def _make_flowstate():
    """Create a _FlowState with standard FlowSpec initialization keys."""
    return _FlowState(
        {
            FlowStateItems.FLOW_MUTATORS: [],
            FlowStateItems.FLOW_DECORATORS: {},
            FlowStateItems.CONFIGS: {},
            FlowStateItems.CACHED_PARAMETERS: None,
            FlowStateItems.SET_CONFIG_PARAMETERS: [],
        }
    )


class TestFlowStateIter:
    """Verify __iter__ yields keys, not values (MutableMapping contract)."""

    def test_iter_yields_keys(self):
        fs = _make_flowstate()
        keys = list(fs)
        for k in keys:
            assert isinstance(
                k, FlowStateItems
            ), f"__iter__ should yield FlowStateItems keys, got {type(k)}: {k}"

    def test_iter_keys_match_self_data(self):
        fs = _make_flowstate()
        assert set(fs) == set(fs._self_data.keys())

    def test_keys_method(self):
        fs = _make_flowstate()
        keys = list(fs.keys())
        assert all(isinstance(k, FlowStateItems) for k in keys)
        assert len(keys) == 5

    def test_items_method(self):
        fs = _make_flowstate()
        for k, v in fs.items():
            assert isinstance(
                k, FlowStateItems
            ), f"items() key should be FlowStateItems, got {type(k)}: {k}"

    def test_contains_uses_keys(self):
        fs = _make_flowstate()
        assert FlowStateItems.FLOW_MUTATORS in fs
        assert FlowStateItems.FLOW_DECORATORS in fs
        # A non-existent key should NOT be found via __contains__
        assert "nonexistent_key" not in fs

    def test_len(self):
        fs = _make_flowstate()
        assert len(fs) == 5
