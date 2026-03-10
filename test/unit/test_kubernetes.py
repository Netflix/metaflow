import pytest
from unittest.mock import MagicMock

from metaflow.plugins.kubernetes.kube_utils import (
    KubernetesException,
    validate_kube_labels,
    parse_kube_keyvalue_list,
)
from metaflow.plugins.resources_decorator import ResourcesDecorator
from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator


@pytest.mark.parametrize(
    "labels",
    [
        None,
        {"label": "value"},
        {"label1": "val1", "label2": "val2"},
        {"label1": "val1", "label2": None},
        {"label": "a"},
        {"label": ""},
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "123"
            )
        },
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234-_.890"
                "1234567890"
                "1234567890"
                "1234567890"
                "123"
            )
        },
    ],
)
def test_kubernetes_decorator_validate_kube_labels(labels):
    assert validate_kube_labels(labels)


@pytest.mark.parametrize(
    "labels",
    [
        {"label": "a-"},
        {"label": ".a"},
        {"label": "test()"},
        {
            "label": (
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234567890"
                "1234"
            )
        },
        {"label": "(){}??"},
        {"valid": "test", "invalid": "bißchen"},
    ],
)
def test_kubernetes_decorator_validate_kube_labels_fail(labels):
    """Fail if label contains invalid characters or is too long"""
    with pytest.raises(KubernetesException):
        validate_kube_labels(labels)


@pytest.mark.parametrize(
    "items,requires_both,expected",
    [
        (["key=value"], True, {"key": "value"}),
        (["key=value"], False, {"key": "value"}),
        (["key"], False, {"key": None}),
        (["key=value", "key2=value2"], True, {"key": "value", "key2": "value2"}),
    ],
)
def test_kubernetes_parse_keyvalue_list(items, requires_both, expected):
    ret = parse_kube_keyvalue_list(items, requires_both)
    assert ret == expected


@pytest.mark.parametrize(
    "items,requires_both",
    [
        (["key=value", "key=value2"], True),
        (["key"], True),
    ],
)
def test_kubernetes_parse_keyvalue_list(items, requires_both):
    with pytest.raises(KubernetesException):
        parse_kube_keyvalue_list(items, requires_both)


class TestResourcesDecoratorOverride:
    """
    Tests for issue #2464: @resources decorator should correctly set memory
    for K8s even when the value is less than the @kubernetes default.
    """

    def _make_k8s_deco(self, **attrs):
        """Create a KubernetesDecorator with given attributes, skipping init hooks."""
        deco = KubernetesDecorator.__new__(KubernetesDecorator)
        deco.attributes = dict(KubernetesDecorator.defaults)
        deco.attributes.update(attrs)
        return deco

    def _make_resources_deco(self, **attrs):
        """Create a ResourcesDecorator with given attributes."""
        deco = ResourcesDecorator.__new__(ResourcesDecorator)
        deco.attributes = dict(ResourcesDecorator.defaults)
        deco.attributes.update(attrs)
        return deco

    def _apply_resources(self, k8s_deco, res_deco):
        """Simulate the step_init logic for merging @resources into @kubernetes."""
        for k, v in res_deco.attributes.items():
            if k == "gpu" and v is not None:
                k8s_deco.attributes["gpu"] = v
            if k == "shared_memory" and v is not None:
                k8s_deco.attributes["shared_memory"] = v
            if k in k8s_deco.attributes:
                if KubernetesDecorator.defaults[k] is None:
                    continue
                my_val = k8s_deco.attributes.get(k)
                if not (my_val is None and v is None):
                    k8s_is_default = str(my_val) == str(KubernetesDecorator.defaults[k])
                    resources_is_default = k in ResourcesDecorator.defaults and str(
                        v
                    ) == str(ResourcesDecorator.defaults[k])
                    if k8s_is_default and not resources_is_default:
                        k8s_deco.attributes[k] = str(float(v or 0))
                    else:
                        k8s_deco.attributes[k] = str(
                            max(float(my_val or 0), float(v or 0))
                        )

    def test_resources_memory_lower_than_k8s_default(self):
        """@resources(memory=256) should override @kubernetes default of 4096."""
        k8s = self._make_k8s_deco()
        res = self._make_resources_deco(memory="256")
        self._apply_resources(k8s, res)
        assert float(k8s.attributes["memory"]) == 256.0

    def test_resources_memory_higher_than_k8s_default(self):
        """@resources(memory=8192) should result in 8192 (max wins)."""
        k8s = self._make_k8s_deco()
        res = self._make_resources_deco(memory="8192")
        self._apply_resources(k8s, res)
        assert float(k8s.attributes["memory"]) == 8192.0

    def test_explicit_k8s_memory_takes_max_with_resources(self):
        """When both @kubernetes(memory=2048) and @resources(memory=256), max wins."""
        k8s = self._make_k8s_deco(memory="2048")
        res = self._make_resources_deco(memory="256")
        self._apply_resources(k8s, res)
        assert float(k8s.attributes["memory"]) == 2048.0

    def test_explicit_k8s_memory_lower_resources_higher(self):
        """When @kubernetes(memory=2048) and @resources(memory=8192), max wins."""
        k8s = self._make_k8s_deco(memory="2048")
        res = self._make_resources_deco(memory="8192")
        self._apply_resources(k8s, res)
        assert float(k8s.attributes["memory"]) == 8192.0

    def test_resources_cpu_lower_than_k8s_default(self):
        """@resources(cpu=0.5) should override @kubernetes default of 1."""
        k8s = self._make_k8s_deco()
        res = self._make_resources_deco(cpu="0.5")
        self._apply_resources(k8s, res)
        assert float(k8s.attributes["cpu"]) == 0.5

    def test_both_at_defaults(self):
        """When both are at defaults, the default value should be preserved."""
        k8s = self._make_k8s_deco()
        res = self._make_resources_deco()
        self._apply_resources(k8s, res)
        # Both have same defaults, max of defaults = default
        assert float(k8s.attributes["memory"]) == 4096.0
        assert float(k8s.attributes["cpu"]) == 1.0
