"""Tests for WorkflowSpec.ttl_strategy()."""

from metaflow.plugins.argo.argo_workflows import WorkflowSpec


def test_positive_value_sets_ttl():
    ws = WorkflowSpec()
    ws.ttl_strategy(604800)
    assert ws.payload["ttlStrategy"] == {"secondsAfterCompletion": 604800}


def test_zero_disables_ttl():
    ws = WorkflowSpec()
    ws.ttl_strategy(0)
    assert "ttlStrategy" not in ws.payload


def test_none_disables_ttl():
    ws = WorkflowSpec()
    ws.ttl_strategy(None)
    assert "ttlStrategy" not in ws.payload


def test_negative_disables_ttl():
    ws = WorkflowSpec()
    ws.ttl_strategy(-1)
    assert "ttlStrategy" not in ws.payload


def test_string_value_converted_to_int():
    ws = WorkflowSpec()
    ws.ttl_strategy("86400")
    assert ws.payload["ttlStrategy"] == {"secondsAfterCompletion": 86400}


def test_chaining():
    """ttl_strategy returns self for method chaining."""
    ws = WorkflowSpec()
    result = ws.ttl_strategy(3600)
    assert result is ws
