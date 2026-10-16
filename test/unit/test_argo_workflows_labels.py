import pytest

from metaflow.plugins.argo.argo_workflows import ArgoWorkflows
from metaflow.plugins.kubernetes.kube_utils import KubernetesException


@pytest.fixture
def argo_workflows():
    return ArgoWorkflows.__new__(ArgoWorkflows)


@pytest.mark.parametrize(
    ("configured_labels", "expected"),
    [
        (
            "",
            {"app.kubernetes.io/part-of": "metaflow"},
        ),
        (
            "team=ml,env=prod",
            {
                "app.kubernetes.io/part-of": "metaflow",
                "team": "ml",
                "env": "prod",
            },
        ),
        (
            "app.kubernetes.io/part-of=custom,team=ml",
            {
                "app.kubernetes.io/part-of": "metaflow",
                "team": "ml",
            },
        ),
    ],
    ids=["default", "custom-labels", "protected-label"],
)
def test_base_argo_labels(mocker, argo_workflows, configured_labels, expected):
    mocker.patch(
        "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
        configured_labels,
    )

    assert argo_workflows._base_argo_labels() == expected


@pytest.mark.parametrize(
    "configured_labels",
    [
        "missing-value",
        "team=value with spaces",
        "team=%s" % ("a" * 64),
    ],
    ids=["missing-equals", "invalid-value", "value-too-long"],
)
def test_base_argo_labels_rejects_invalid_configuration(
    mocker, argo_workflows, configured_labels
):
    mocker.patch(
        "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
        configured_labels,
    )

    with pytest.raises(KubernetesException):
        argo_workflows._base_argo_labels()
