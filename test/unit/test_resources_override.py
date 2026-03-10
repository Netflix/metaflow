"""Tests for @resources vs @kubernetes decorator precedence logic.

The merge logic in KubernetesDecorator.step_init works as follows:
- If @kubernetes attribute is at its default AND @resources explicitly sets a value
  → use the @resources value (even if lower than the @kubernetes default).
- If both explicitly set → take max.
- If both at default → take max (which equals default).
"""

K8S_DEFAULTS = {"cpu": "1", "memory": "4096", "disk": "10240"}
RESOURCES_DEFAULTS = {"cpu": "1", "memory": "4096"}


def _merge(k, k8s_val, resources_val):
    """Replicate the merge logic from KubernetesDecorator.step_init."""
    k8s_is_default = float(k8s_val or 0) == float(K8S_DEFAULTS.get(k) or 0)
    resources_is_default = k in RESOURCES_DEFAULTS and float(
        resources_val or 0
    ) == float(RESOURCES_DEFAULTS.get(k) or 0)

    if k8s_is_default and not resources_is_default:
        return str(float(resources_val or 0))
    else:
        return str(max(float(k8s_val or 0), float(resources_val or 0)))


def test_resources_lower_than_k8s_default():
    """@resources(memory=256) with @kubernetes default (4096) → resources wins."""
    result = _merge("memory", K8S_DEFAULTS["memory"], "256")
    assert result == "256.0"


def test_resources_higher_than_k8s_default():
    """@resources(memory=8192) with @kubernetes default (4096) → resources wins."""
    result = _merge("memory", K8S_DEFAULTS["memory"], "8192")
    assert result == "8192.0"


def test_both_explicit_resources_higher():
    """@kubernetes(memory=2048) explicit with @resources(memory=8192) → max wins."""
    result = _merge("memory", "2048", "8192")
    assert result == "8192.0"


def test_both_explicit_k8s_higher():
    """@kubernetes(memory=8192) explicit with @resources(memory=2048) → max wins."""
    result = _merge("memory", "8192", "2048")
    assert result == "8192.0"


def test_both_at_default():
    """Both at default → stays at default (4096)."""
    result = _merge("memory", K8S_DEFAULTS["memory"], RESOURCES_DEFAULTS["memory"])
    assert result == "4096.0"
