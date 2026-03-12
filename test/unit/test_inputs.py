import pytest
from unittest.mock import MagicMock
from collections import namedtuple

from metaflow.datastore.inputs import Inputs


# Replicate ForeachFrame structure for testing
ForeachFrame = namedtuple(
    "ForeachFrame", ["step", "var", "num_splits", "index", "value"]
)


def _make_mock_flow(step_name, foreach_index=None, foreach_step="process"):
    """Create a mock flow object that mimics a cloned flow with a datastore.

    Parameters
    ----------
    step_name : str
        The step name to set as _current_step.
    foreach_index : int or None
        If not None, the flow will have a _foreach_stack with this index.
    foreach_step : str
        The step name in the ForeachFrame.
    """
    flow = MagicMock()
    flow._current_step = step_name

    if foreach_index is not None:
        frame = ForeachFrame(
            step=foreach_step,
            var="items",
            num_splits=5,
            index=foreach_index,
            value=str(foreach_index),
        )
        flow._datastore = {"_foreach_stack": [frame]}
    else:
        flow._datastore = {}

    return flow


class TestInputsForeachSorting:
    """Test that Inputs sorts flows by foreach index for foreach joins."""

    def test_foreach_inputs_sorted_by_index(self):
        """Inputs from a foreach join should be sorted by foreach index,
        regardless of the order they are provided."""
        # Create flows in reverse order (4, 3, 2, 1, 0)
        flows = [_make_mock_flow("process", foreach_index=i) for i in [4, 3, 2, 1, 0]]

        inputs = Inputs(flows)

        # Should be sorted: 0, 1, 2, 3, 4
        indices = [f._datastore["_foreach_stack"][-1].index for f in inputs]
        assert indices == [0, 1, 2, 3, 4]

    def test_foreach_inputs_already_sorted(self):
        """Inputs that are already sorted should remain sorted."""
        flows = [_make_mock_flow("process", foreach_index=i) for i in range(5)]

        inputs = Inputs(flows)

        indices = [f._datastore["_foreach_stack"][-1].index for f in inputs]
        assert indices == [0, 1, 2, 3, 4]

    def test_foreach_inputs_shuffled_order(self):
        """Inputs in arbitrary order should be sorted by foreach index."""
        flows = [_make_mock_flow("process", foreach_index=i) for i in [2, 0, 4, 1, 3]]

        inputs = Inputs(flows)

        indices = [f._datastore["_foreach_stack"][-1].index for f in inputs]
        assert indices == [0, 1, 2, 3, 4]

    def test_foreach_inputs_accessible_by_index(self):
        """inputs[i] should correspond to foreach index i."""
        flows = [_make_mock_flow("process", foreach_index=i) for i in [2, 0, 1]]

        inputs = Inputs(flows)

        assert inputs[0]._datastore["_foreach_stack"][-1].index == 0
        assert inputs[1]._datastore["_foreach_stack"][-1].index == 1
        assert inputs[2]._datastore["_foreach_stack"][-1].index == 2


class TestInputsNonForeachPreserved:
    """Test that non-foreach joins are not affected by sorting."""

    def test_static_split_inputs_preserve_order(self):
        """For static splits (no foreach), original order should be preserved."""
        flow_a = _make_mock_flow("step_a")
        flow_b = _make_mock_flow("step_b")

        inputs = Inputs([flow_a, flow_b])

        # Order preserved, named access works
        assert inputs[0]._current_step == "step_a"
        assert inputs[1]._current_step == "step_b"
        assert inputs.step_a._current_step == "step_a"
        assert inputs.step_b._current_step == "step_b"

    def test_empty_inputs(self):
        """Empty inputs should not raise."""
        inputs = Inputs([])
        assert list(inputs) == []


class TestInputsIteration:
    """Test that iteration over Inputs respects the sorted order."""

    def test_iter_foreach_sorted(self):
        """Iterating over foreach Inputs should yield sorted order."""
        flows = [_make_mock_flow("process", foreach_index=i) for i in [2, 0, 1]]

        inputs = Inputs(flows)

        indices = [f._datastore["_foreach_stack"][-1].index for f in inputs]
        assert indices == [0, 1, 2]

    def test_len_preserved(self):
        """Number of inputs should be preserved after sorting."""
        flows = [_make_mock_flow("process", foreach_index=i) for i in [3, 1, 2, 0]]

        inputs = Inputs(flows)

        assert len(list(inputs)) == 4
