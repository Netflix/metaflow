import pytest

from metaflow.plugins.kubernetes.kubernetes import (
    KubernetesException,
    validate_kube_labels,
    parse_kube_keyvalue_list,
)


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
