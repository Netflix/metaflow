"""Tests for _FlowState.__delitem__ correctness.

_FlowState and FlowStateItems only depend on stdlib (MutableMapping, Enum),
but importing metaflow.flowspec triggers transitive imports that may fail on
some platforms (e.g. fcntl on Windows).  We use pytest.importorskip so that
only ImportError / ModuleNotFoundError cause a skip — any other exception
(e.g. a real regression in flowspec) will still surface as a test failure.
"""

import pytest

_flowspec = pytest.importorskip(
    "metaflow.flowspec",
    reason="metaflow.flowspec is not importable on this platform",
)
_FlowState = _flowspec._FlowState
FlowStateItems = _flowspec.FlowStateItems


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


class TestFlowStateDelItem:
    """Verify __delitem__ correctly removes keys from all backing stores."""

    def test_delete_non_inherited_key(self):
        """Deleting a non-inherited key (e.g. CONFIGS) should not raise KeyError."""
        fs = _make_flowstate()
        del fs[FlowStateItems.CONFIGS]
        assert FlowStateItems.CONFIGS not in fs._self_data
        assert len(fs) == 4

    def test_delete_inherited_key(self):
        """Deleting an inherited key should remove it from _self_data so it
        cannot be recomputed on subsequent access."""
        fs = _make_flowstate()
        del fs[FlowStateItems.FLOW_MUTATORS]
        assert FlowStateItems.FLOW_MUTATORS not in fs._self_data
        with pytest.raises(KeyError):
            _ = fs[FlowStateItems.FLOW_MUTATORS]

    def test_delete_inherited_key_clears_merged_cache(self):
        """If a merged value was cached, deleting the key should evict it."""
        fs = _make_flowstate()
        # Force a cached merge by accessing the key.
        _ = fs[FlowStateItems.FLOW_DECORATORS]
        assert FlowStateItems.FLOW_DECORATORS in fs._merged_data

        del fs[FlowStateItems.FLOW_DECORATORS]
        assert FlowStateItems.FLOW_DECORATORS not in fs._merged_data
        assert FlowStateItems.FLOW_DECORATORS not in fs._self_data

    def test_delete_missing_key_raises(self):
        """Deleting a key that doesn't exist should raise KeyError."""
        fs = _make_flowstate()
        with pytest.raises(KeyError):
            del fs["no_such_key"]

    def test_len_after_delete(self):
        fs = _make_flowstate()
        original_len = len(fs)
        del fs[FlowStateItems.CACHED_PARAMETERS]
        assert len(fs) == original_len - 1
