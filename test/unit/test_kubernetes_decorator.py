import pytest

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
    cleaned_labels = KubernetesDecorator.validate_kube_labels(labels)
    assert cleaned_labels == labels


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
    with pytest.raises(Exception):
        KubernetesDecorator.validate_kube_labels(labels)
