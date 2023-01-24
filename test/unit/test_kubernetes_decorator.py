import pytest

from metaflow.plugins.kubernetes.kubernetes_decorator import KubernetesDecorator


@pytest.mark.parametrize(
    "labels,expected",
    [
        (None, None),
        ({"label": "value"}, {"label": "value"}),
        ({"label1": "val1", "label2": "val2"}, {"label1": "val1", "label2": "val2"}),
        ({"label1": "val1", "label2": None}, {"label1": "val1", "label2": None}),
        ({"label": "test()"}, {"label": "test-1ba883287c"}),
        (
            {
                "label": "reallyreallyreallyreallyreallyreallyreallyreallyreallyreallyreallylong"
            },
            {
                "label": "reallyreallyreallyreallyreallyreallyreallyreallyreal-768ebdbe62"
            },
        ),
    ],
)
def test_kubernetes_decorator_clean_kube_labels(labels, expected):
    cleaned_labels = KubernetesDecorator.clean_kube_labels(labels)
    assert cleaned_labels == expected


def test_kubernetes_decorator_clean_kube_labels_fail():
    """Fail if label contains no valid characters"""
    labels = {"labels": "(){}??"}
    with pytest.raises(AssertionError):
        KubernetesDecorator.clean_kube_labels(labels)
