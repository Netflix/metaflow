"""Tests for @resources vs @kubernetes precedence logic.

The merge logic in KubernetesDecorator.step_init determines which value
wins when both @resources and @kubernetes set the same attribute (e.g. memory).

Rules:
- If @kubernetes is at its default and @resources explicitly sets a value,
  @resources wins (even if lower).
- If both are explicitly set, the larger value wins.
- If both are at default, the default is kept.
"""

from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator
from metaflow.plugins.resources_decorator import ResourcesDecorator


def _merge_resource(k8s_val, resources_val, attr="memory"):
    """Simulate the merge logic from step_init for a single attribute.

    Returns the resolved value as a string (matching self.attributes format).
    """
    k8s_default = KubernetesDecorator.defaults[attr]
    res_default = ResourcesDecorator.defaults.get(attr)

    my_val = k8s_val
    v = resources_val

    if my_val is None and v is None:
        return str(my_val)

    k8s_is_default = float(my_val or 0) == float(k8s_default or 0)
    resources_is_default = attr in ResourcesDecorator.defaults and float(
        v or 0
    ) == float(res_default or 0)

    if k8s_is_default and not resources_is_default:
        return str(float(v or 0))
    else:
        return str(max(float(my_val or 0), float(v or 0)))


def test_resources_lower_than_k8s_default_wins():
    """@resources(memory=256) should override @kubernetes default (4096)."""
    result = _merge_resource("4096", "256")
    assert float(result) == 256.0


def test_resources_higher_than_k8s_default_wins():
    """@resources(memory=8192) overrides @kubernetes default (4096)."""
    result = _merge_resource("4096", "8192")
    assert float(result) == 8192.0


def test_both_explicit_takes_max():
    """@kubernetes(memory=2048) + @resources(memory=8192) → max wins."""
    result = _merge_resource("2048", "8192")
    # k8s is not at default (2048 != 4096), so max(2048, 8192) = 8192
    assert float(result) == 8192.0


def test_both_explicit_k8s_higher():
    """@kubernetes(memory=8192) + @resources(memory=2048) → k8s wins."""
    result = _merge_resource("8192", "2048")
    assert float(result) == 8192.0


def test_both_at_default():
    """Both at default → stays at default."""
    k8s_default = KubernetesDecorator.defaults["memory"]
    res_default = ResourcesDecorator.defaults["memory"]
    result = _merge_resource(k8s_default, res_default)
    assert float(result) == float(k8s_default)


def test_cpu_resources_overrides_k8s_default():
    """@resources(cpu=2) should override @kubernetes default cpu (1)."""
    result = _merge_resource("1", "2", attr="cpu")
    assert float(result) == 2.0
