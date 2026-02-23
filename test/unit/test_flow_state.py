import pytest

from metaflow.flowspec import _FlowState, FlowStateItems


def _make_flow_state():
    """Create a _FlowState with realistic initial data for all FlowStateItems keys."""
    init_data = {
        FlowStateItems.FLOW_MUTATORS: [],
        FlowStateItems.FLOW_DECORATORS: [],
        FlowStateItems.CONFIGS: {},
        FlowStateItems.CACHED_PARAMETERS: {},
        FlowStateItems.SET_CONFIG_PARAMETERS: {},
    }
    return _FlowState(init_data)


class TestFlowStateIter:
    def test_iter_yields_keys(self):
        fs = _make_flow_state()
        keys = list(fs)
        for k in keys:
            assert isinstance(k, FlowStateItems)

    def test_keys_method(self):
        fs = _make_flow_state()
        assert set(fs.keys()) == set(FlowStateItems)

    def test_values_method(self):
        fs = _make_flow_state()
        vals = list(fs.values())
        assert len(vals) == len(FlowStateItems)
        for v in vals:
            assert isinstance(v, (list, dict))

    def test_items_method(self):
        fs = _make_flow_state()
        items = list(fs.items())
        assert len(items) == len(FlowStateItems)
        for k, v in items:
            assert isinstance(k, FlowStateItems)
            assert fs[k] is v

    def test_contains_checks_keys(self):
        fs = _make_flow_state()
        assert FlowStateItems.FLOW_DECORATORS in fs
        assert FlowStateItems.CONFIGS in fs
        assert "garbage" not in fs

    def test_dict_conversion(self):
        fs = _make_flow_state()
        d = dict(fs)
        assert isinstance(d, dict)
        assert set(d.keys()) == set(FlowStateItems)
        for k, v in d.items():
            assert isinstance(k, FlowStateItems)

    def test_len(self):
        fs = _make_flow_state()
        assert len(fs) == len(FlowStateItems)

    def test_delitem_safe_on_uncached(self):
        fs = _make_flow_state()
        # CONFIGS is a non-inherited item; delete without accessing first
        # should not raise even though _merged_data has no entry for it
        del fs[FlowStateItems.CONFIGS]
        assert FlowStateItems.CONFIGS not in fs._self_data
