from metaflow.plugins.argo.argo_workflows import ArgoWorkflows


def test_sensor_name_short():
    """Short names should pass through with only dot replacement."""
    assert ArgoWorkflows._sensor_name("my.flow.name") == "my-flow-name"


def test_sensor_name_no_dots():
    """Names without dots should be unchanged."""
    assert ArgoWorkflows._sensor_name("myflowname") == "myflowname"


def test_sensor_name_long_truncated():
    """Names exceeding 253 chars should be truncated with a hash suffix."""
    long_name = "a" * 300
    result = ArgoWorkflows._sensor_name(long_name)
    assert len(result) <= 253
    # Should contain a hash suffix
    assert "-" in result[240:]
    # Hash portion should be 12 hex chars
    hash_part = result.split("-")[-1]
    assert len(hash_part) == 12
    assert all(c in "0123456789abcdef" for c in hash_part)


def test_sensor_name_exactly_253():
    """A name exactly 253 chars should not be truncated."""
    name = "a" * 253
    result = ArgoWorkflows._sensor_name(name)
    assert result == name
    assert len(result) == 253


def test_sensor_name_254():
    """A name of 254 chars should be truncated."""
    name = "a" * 254
    result = ArgoWorkflows._sensor_name(name)
    assert len(result) <= 253


def test_sensor_name_long_deterministic():
    """Same long input should always produce the same truncated name."""
    long_name = "project.branch." + "x" * 300 + ".FlowName"
    result1 = ArgoWorkflows._sensor_name(long_name)
    result2 = ArgoWorkflows._sensor_name(long_name)
    assert result1 == result2


def test_sensor_name_long_unique():
    """Different long inputs should produce different truncated names."""
    name1 = "a" * 300
    name2 = "b" * 300
    result1 = ArgoWorkflows._sensor_name(name1)
    result2 = ArgoWorkflows._sensor_name(name2)
    assert result1 != result2
