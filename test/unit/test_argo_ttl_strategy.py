from metaflow.plugins.argo.argo_workflows import WorkflowSpec


def test_positive_value():
    spec = WorkflowSpec()
    spec.ttl_strategy(86400)
    assert spec.payload["ttlStrategy"] == {"secondsAfterCompletion": 86400}


def test_zero_no_ttl():
    spec = WorkflowSpec()
    spec.ttl_strategy(0)
    assert "ttlStrategy" not in spec.payload


def test_none_no_ttl():
    spec = WorkflowSpec()
    spec.ttl_strategy(None)
    assert "ttlStrategy" not in spec.payload


def test_string_value_converted_to_int():
    spec = WorkflowSpec()
    spec.ttl_strategy("86400")
    assert spec.payload["ttlStrategy"] == {"secondsAfterCompletion": 86400}


def test_negative_value_no_ttl():
    spec = WorkflowSpec()
    spec.ttl_strategy(-1)
    assert "ttlStrategy" not in spec.payload
