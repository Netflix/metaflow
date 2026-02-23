"""
Tests for _FlowState class.

Tests that _FlowState correctly implements the MutableMapping contract,
including __iter__ yielding keys (not values) as required by the ABC.
"""

from collections.abc import MutableMapping

from metaflow.flowspec import _FlowState, FlowStateItems


class TestFlowStateMutableMapping:
    """Test that _FlowState correctly implements MutableMapping contract"""

    def test_is_mutable_mapping(self):
        """Test that _FlowState is a MutableMapping"""
        state = _FlowState()
        assert isinstance(state, MutableMapping)

    def test_iter_yields_keys_not_values(self):
        """
        Test that __iter__ yields keys, not values.
        
        This is a regression test for GitHub issue #2837.
        The MutableMapping contract requires __iter__ to yield keys.
        """
        # Create a _FlowState with initialized keys (matching actual usage in FlowSpec)
        state = _FlowState()
        state._self_data = {
            FlowStateItems.FLOW_MUTATORS: [],
            FlowStateItems.FLOW_DECORATORS: {},
            FlowStateItems.CONFIGS: {},
            FlowStateItems.CACHED_PARAMETERS: None,
            FlowStateItems.SET_CONFIG_PARAMETERS: [],
        }
        
        # __iter__ should yield keys (FlowStateItems enum values)
        keys_from_iter = list(state)
        
        # Verify that we got the keys, not the values
        for key in keys_from_iter:
            assert isinstance(key, FlowStateItems), (
                f"Expected key to be FlowStateItems enum, got {type(key)}: {key}"
            )
        
        # Verify all expected keys are present
        expected_keys = set(state._self_data.keys())
        actual_keys = set(keys_from_iter)
        assert actual_keys == expected_keys, (
            f"Keys from __iter__ don't match. Expected: {expected_keys}, Got: {actual_keys}"
        )

    def test_iter_yields_keys_with_inheritance(self):
        """
        Test that __iter__ yields keys even when inherited data is present.
        """
        state = _FlowState()
        state._self_data = {
            FlowStateItems.FLOW_MUTATORS: [],
            FlowStateItems.FLOW_DECORATORS: {},
            FlowStateItems.CONFIGS: {},
            FlowStateItems.CACHED_PARAMETERS: None,
            FlowStateItems.SET_CONFIG_PARAMETERS: [],
        }
        # Add inherited data
        state._inherited = {
            FlowStateItems.FLOW_DECORATORS: {"inherited_decorator": {}},
            FlowStateItems.CONFIGS: {"inherited_config": {}},
        }
        
        # __iter__ should still yield keys from _self_data
        keys_from_iter = list(state)
        
        for key in keys_from_iter:
            assert isinstance(key, FlowStateItems), (
                f"Expected key to be FlowStateItems enum, got {type(key)}"
            )
        
        expected_keys = set(state._self_data.keys())
        actual_keys = set(keys_from_iter)
        assert actual_keys == expected_keys

    def test_dict_comprehension_works(self):
        """
        Test that dict-like operations work correctly.
        
        If __iter__ yields values instead of keys, dict(state) would fail
        or produce incorrect results.
        """
        state = _FlowState()
        state._self_data = {
            FlowStateItems.FLOW_MUTATORS: [],
            FlowStateItems.FLOW_DECORATORS: {"dec": {}},
            FlowStateItems.CONFIGS: {},
            FlowStateItems.CACHED_PARAMETERS: None,
            FlowStateItems.SET_CONFIG_PARAMETERS: [],
        }
        
        # This should work if __iter__ yields keys
        # If __iter__ yielded values, this would fail or produce wrong results
        result = {k: state[k] for k in state}
        
        # All keys should be present
        assert FlowStateItems.FLOW_MUTATORS in result
        assert FlowStateItems.FLOW_DECORATORS in result
        assert FlowStateItems.CONFIGS in result
