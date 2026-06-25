"""
Tests for MetaflowObject.__init__ _metaflow parameter handling.

Regression tests for https://github.com/Netflix/metaflow/issues/3019

The bug: `self._metaflow = Metaflow(_current_metadata) or _metaflow`
always evaluated to the new Metaflow() because it is always truthy,
so the passed-in _metaflow was silently discarded.

The fix: `self._metaflow = _metaflow or Metaflow(_current_metadata)`
prefers the caller-supplied instance and only falls back to constructing
a new one when _metaflow is None.
"""

from unittest.mock import patch, MagicMock

from metaflow.client.core import Metaflow


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _DummyMetaflow:
    """
    A lightweight stand-in for Metaflow that is always truthy and carries a
    mock metadata attribute with all the methods MetaflowObject needs.
    """

    def __init__(self):
        self.metadata = MagicMock()
        self.metadata.get_object.return_value = {
            "flow_id": "TestFlow",
            "run_number": "1",
            "step_name": "start",
            "task_id": "1",
            "name": "artifact",
            "ts_epoch": 1700000000000,
            "tags": [],
            "system_tags": [],
        }
        self.metadata.metadata_str.return_value = "local@."

    def __bool__(self):
        return True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMetaflowObjectMetaflowParam:
    """Verify that _metaflow parameter is respected in MetaflowObject.__init__."""

    def test_passed_metaflow_is_used(self):
        """When _metaflow is passed, self._metaflow must be that exact instance."""
        passed_mf = _DummyMetaflow()

        from metaflow.client.core import Flow

        with patch.object(Flow, "_get_object", return_value={
            "flow_id": "TestFlow",
            "ts_epoch": 1700000000000,
            "tags": [],
            "system_tags": [],
        }):
            flow = Flow(
                pathspec="TestFlow",
                _metaflow=passed_mf,
                _namespace_check=False,
            )

        # The critical assertion: self._metaflow must be the *passed-in* instance,
        # not a freshly constructed Metaflow().
        assert flow._metaflow is passed_mf, (
            "_metaflow parameter was ignored — a new Metaflow() was constructed "
            "instead. This is the bug described in issue #3019."
        )

    def test_no_metaflow_falls_back_to_new_instance(self):
        """When _metaflow is None (default), a new Metaflow() should be constructed."""
        from metaflow.client.core import Flow

        with patch.object(Flow, "_get_object", return_value={
            "flow_id": "TestFlow",
            "ts_epoch": 1700000000000,
            "tags": [],
            "system_tags": [],
        }):
            flow = Flow(
                pathspec="TestFlow",
                _namespace_check=False,
            )

        # _metaflow should be a real Metaflow instance, not None
        assert isinstance(flow._metaflow, Metaflow), (
            "When _metaflow is not passed, a default Metaflow() should be "
            "constructed."
        )

    def test_run_stores_passed_metaflow(self):
        """When _metaflow is passed to Run directly, it must be stored as-is."""
        parent_mf = _DummyMetaflow()

        from metaflow.client.core import Run

        with patch.object(Run, "_get_object", return_value={
            "run_number": "42",
            "ts_epoch": 1700000000000,
            "tags": [],
            "system_tags": [],
        }):
            run = Run(
                pathspec="TestFlow/42",
                _metaflow=parent_mf,
                _namespace_check=False,
            )

        # The run should hold the parent's _metaflow, not a new one
        assert run._metaflow is parent_mf, (
            "Child object did not inherit parent's _metaflow instance."
        )

    def test_passed_metaflow_takes_precedence_over_current_metadata(self):
        """When both _metaflow and _current_metadata are given, _metaflow wins."""
        passed_mf = _DummyMetaflow()

        from metaflow.client.core import Flow

        with patch.object(Flow, "_get_object", return_value={
            "flow_id": "TestFlow",
            "ts_epoch": 1700000000000,
            "tags": [],
            "system_tags": [],
        }):
            flow = Flow(
                pathspec="TestFlow",
                _metaflow=passed_mf,
                _current_metadata="local@/tmp/should_be_ignored",
                _namespace_check=False,
            )

        assert flow._metaflow is passed_mf, (
            "When both _metaflow and _current_metadata are provided, "
            "_metaflow should take precedence."
        )

    def test_getitem_passes_parent_metaflow(self):
        """Flow.__getitem__ should pass self._metaflow to the child Run."""
        parent_mf = _DummyMetaflow()

        from metaflow.client.core import Flow

        child_obj = {
            "run_number": "99",
            "ts_epoch": 1700000000000,
            "tags": [],
            "system_tags": [],
        }

        with patch.object(Flow, "_get_object", return_value={
            "flow_id": "TestFlow",
            "ts_epoch": 1700000000000,
            "tags": [],
            "system_tags": [],
        }):
            flow = Flow(
                pathspec="TestFlow",
                _metaflow=parent_mf,
                _namespace_check=False,
            )

        # Patch _get_child so __getitem__ finds something
        with patch.object(Flow, "_get_child", return_value=child_obj):
            run = flow["99"]

        assert run._metaflow is parent_mf, (
            "Flow.__getitem__ did not propagate _metaflow to child Run."
        )
