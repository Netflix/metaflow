import pytest
from kubernetes.client.rest import ApiException

from metaflow.plugins.argo.argo_client import ArgoClient, ArgoClientException


@pytest.fixture
def argo_client(mocker):
    mock_custom_objects_api = mocker.Mock()
    mock_k8s = mocker.Mock()
    mock_k8s.CustomObjectsApi.return_value = mock_custom_objects_api
    mock_k8s.rest.ApiException = ApiException

    mock_kubernetes_client = mocker.Mock()
    mock_kubernetes_client.get.return_value = mock_k8s

    mocker.patch(
        "metaflow.plugins.argo.argo_client.KubernetesClient",
        return_value=mock_kubernetes_client,
    )

    client = ArgoClient(namespace="test-ns")
    return client, mock_custom_objects_api


def test_get_workflow_templates_404_raises(argo_client):
    client, mock_api = argo_client
    api_error = ApiException(status=404, reason="Not Found")
    api_error.body = '{"message": "workflowtemplates.argoproj.io not found"}'
    mock_api.list_namespaced_custom_object.side_effect = api_error

    with pytest.raises(ArgoClientException):
        list(client.get_workflow_templates())


def test_get_workflow_templates_empty_200_does_not_raise(argo_client):
    client, mock_api = argo_client
    mock_api.list_namespaced_custom_object.return_value = {
        "items": [],
        "metadata": {},
    }

    result = list(client.get_workflow_templates())
    assert result == []
