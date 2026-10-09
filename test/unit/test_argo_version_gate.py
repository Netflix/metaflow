"""Deploy-time gate for Argo versions on which conditional steps hang
(argoproj/argo-workflows#15442, fixed by #15841): 3.7.11-3.7.12, 4.0.2-4.0.3.
"""

import json

import pytest

from metaflow import FlowSpec, step
from metaflow.plugins.argo.argo_client import ArgoClient
from metaflow.plugins.argo.argo_workflows import (
    ArgoWorkflows,
    ArgoWorkflowsException,
    _argo_version_breaks_conditionals,
    _parse_argo_version,
)


class ConditionalFlow(FlowSpec):
    @step
    def start(self):
        self.route = "a"
        self.next({"a": self.a, "b": self.b}, condition="route")

    @step
    def a(self):
        self.next(self.join)

    @step
    def b(self):
        self.next(self.join)

    @step
    def join(self):
        self.next(self.end)

    @step
    def end(self):
        pass


class LinearFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass


# ── version parsing and ranges ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("3.7.11", (3, 7, 11)),
        ("v3.7.16", (3, 7, 16)),
        ("v4.0.4", (4, 0, 4)),
        ("v3.7.11-rc1", (3, 7, 11)),
        ("3.7.12+abc", (3, 7, 12)),
        ("latest", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_argo_version(raw, expected):
    assert _parse_argo_version(raw) == expected


@pytest.mark.parametrize(
    "version, broken",
    [
        ((3, 6, 0), False),
        ((3, 7, 10), False),
        ((3, 7, 11), True),
        ((3, 7, 12), True),
        ((3, 7, 13), False),
        ((3, 7, 18), False),
        ((4, 0, 1), False),
        ((4, 0, 2), True),
        ((4, 0, 3), True),
        ((4, 0, 4), False),
        ((4, 1, 0), False),
        (None, False),
    ],
)
def test_argo_version_breaks_conditionals(version, broken):
    assert _argo_version_breaks_conditionals(version) is broken


# ── ArgoClient.get_server_version() ──────────────────────────────────────────


class _Response:
    def __init__(self, body):
        self._body = body.encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


def _deployment(mocker, image):
    container = mocker.MagicMock(image=image)
    deployment = mocker.MagicMock()
    deployment.spec.template.spec.containers = [container]
    return deployment


@pytest.fixture
def client(mocker):
    argo_client = ArgoClient.__new__(ArgoClient)
    argo_client._client = mocker.MagicMock()
    mocker.patch("metaflow.plugins.argo.argo_client.KUBERNETES_NAMESPACE", "default")
    return argo_client


def _apps(client):
    return client._client.get().AppsV1Api()


def test_version_from_server_api(mocker, client):
    mocker.patch(
        "metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL", "https://argo/"
    )
    urlopen = mocker.patch(
        "urllib.request.urlopen",
        return_value=_Response(json.dumps({"version": "v3.7.11"})),
    )

    assert client.get_server_version() == "v3.7.11"
    # call_args.args/.kwargs need Python 3.8+; unpack the tuple instead.
    args, kwargs = urlopen.call_args
    assert args[0] == "https://argo/api/v1/version"
    # Default TLS verification - no custom (unverified) SSL context.
    assert "context" not in kwargs


def test_falls_back_to_deployment_by_name(mocker, client):
    mocker.patch(
        "metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL", "https://argo"
    )
    mocker.patch("urllib.request.urlopen", side_effect=OSError("unreachable"))
    _apps(client).read_namespaced_deployment.return_value = _deployment(
        mocker, "quay.io/argoproj/workflow-controller:v3.7.12"
    )

    assert client.get_server_version() == "v3.7.12"


def test_finds_helm_deployment_by_label(mocker, client):
    mocker.patch("metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL", None)
    apps = _apps(client)
    apps.read_namespaced_deployment.side_effect = Exception("not found")

    def _list(namespace, label_selector):
        assert label_selector == "app.kubernetes.io/component=workflow-controller"
        items = (
            [_deployment(mocker, "quay.io/argoproj/workflow-controller:v4.0.3")]
            if namespace == "argo"
            else []
        )
        return mocker.MagicMock(items=items)

    apps.list_namespaced_deployment.side_effect = _list

    assert client.get_server_version() == "v4.0.3"


@pytest.mark.parametrize(
    "image",
    [
        "argoproj/workflow-controller:latest",
        "argoproj/workflow-controller@sha256:abc123",
        "registry:5000/argoproj/workflow-controller",
    ],
    ids=["latest", "digest", "registry_port_no_tag"],
)
def test_ignores_non_semver_image_tags(mocker, client, image):
    mocker.patch("metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL", None)
    apps = _apps(client)
    apps.read_namespaced_deployment.return_value = _deployment(mocker, image)
    apps.list_namespaced_deployment.return_value = mocker.MagicMock(items=[])

    assert client.get_server_version() is None


def test_returns_none_when_undetectable(mocker, client):
    mocker.patch("metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL", None)
    apps = _apps(client)
    apps.read_namespaced_deployment.side_effect = Exception("forbidden")
    apps.list_namespaced_deployment.side_effect = Exception("forbidden")

    assert client.get_server_version() is None


# ── deploy() gate ────────────────────────────────────────────────────────────


def _make_argo(mocker, flow_cls, version):
    mocker.patch.object(ArgoWorkflows, "_compile_workflow_template", return_value=None)
    mocker.patch.object(ArgoWorkflows, "_compile_sensor", return_value=None)
    mocker.patch.object(ArgoWorkflows, "cleanup_previous_sensors")
    # Patch at the call site so no kubeconfig is needed.
    argo_client = mocker.patch("metaflow.plugins.argo.argo_workflows.ArgoClient")
    argo_client.return_value.get_server_version.return_value = version
    aw = ArgoWorkflows(
        name="flow",
        graph=flow_cls._graph,
        flow=flow_cls(use_cli=False),
        code_package_metadata={},
        code_package_sha="sha",
        code_package_url="s3://metaflow/test",
        production_token="token",
        metadata=None,
        flow_datastore=None,
        environment=None,
        event_logger=None,
        monitor=None,
        username="test-user",
        enable_heartbeat_daemon=False,
    )
    aw._workflow_template = mocker.MagicMock()
    return aw, argo_client.return_value


@pytest.mark.parametrize("version", ["v3.7.11", "v3.7.12", "v4.0.2", "v4.0.3"])
def test_deploy_rejects_broken_version(mocker, version):
    aw, client = _make_argo(mocker, ConditionalFlow, version)

    with pytest.raises(ArgoWorkflowsException) as exc_info:
        aw.deploy()

    message = str(exc_info.value)
    assert version in message
    assert all(step in message for step in aw.conditional_nodes)
    client.register_workflow_template.assert_not_called()
    aw.cleanup_previous_sensors.assert_not_called()


@pytest.mark.parametrize("version", ["v3.6.0", "v3.7.13", "v3.7.18", "v4.0.4", None])
def test_deploy_allows_supported_or_unknown_version(mocker, version):
    aw, client = _make_argo(mocker, ConditionalFlow, version)

    aw.deploy()

    client.register_workflow_template.assert_called_once()


def test_deploy_skips_check_without_conditionals(mocker):
    aw, client = _make_argo(mocker, LinearFlow, "v3.7.11")

    aw.deploy()

    client.get_server_version.assert_not_called()
    client.register_workflow_template.assert_called_once()
