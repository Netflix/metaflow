"""Unit tests for the Argo Workflows version gate.

The gate rejects flows that use conditional (@switch) steps when deployed
against Argo versions known to have the output-parameter requeue bug
(argoproj/argo-workflows#15932):

    v3: [3.7.11, 3.7.16)
    v4: [4.0.0,  4.0.7)

Flows without conditional steps are never affected.

The version is auto-detected at deploy time via ArgoClient.get_server_version()
(Argo Server REST API → workflow-controller Deployment image tag).  When
detection fails the gate is skipped entirely: unknown versions are treated as
safe so that environments we cannot introspect are never blocked.
"""

import json
import pytest

from metaflow import FlowSpec, step
from metaflow.plugins.argo.argo_client import ArgoClient
from metaflow.plugins.argo.argo_workflows import (
    ArgoWorkflows,
    ArgoWorkflowsException,
    _argo_version_has_conditional_bug,
    _parse_argo_version,
)


# ---------------------------------------------------------------------------
# Helper flows
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# _parse_argo_version
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("3.7.11", (3, 7, 11)),
        ("v3.7.16", (3, 7, 16)),
        ("4.0.7", (4, 0, 7)),
        ("v4.0.0", (4, 0, 0)),
        # extra patch segments are truncated to 3
        ("3.7.11.1", (3, 7, 11)),
        # edge cases that should return None
        (None, None),
        ("", None),
        ("not-a-version", None),
    ],
    ids=[
        "plain_v3",
        "v_prefix_v3",
        "plain_v4",
        "v_prefix_v4",
        "four_part_truncated",
        "none",
        "empty_string",
        "garbage",
    ],
)
def test_parse_argo_version(raw, expected):
    assert _parse_argo_version(raw) == expected


# ---------------------------------------------------------------------------
# _argo_version_has_conditional_bug
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "version_tuple, expected",
    [
        # v3 broken range [3.7.11, 3.7.16)
        ((3, 7, 11), True),
        ((3, 7, 13), True),
        ((3, 7, 15), True),
        # v3 boundary: first fixed version
        ((3, 7, 16), False),
        # v3 safe versions
        ((3, 7, 10), False),
        ((3, 6, 0), False),
        ((3, 8, 0), False),
        # v4 broken range [4.0.0, 4.0.7)
        ((4, 0, 0), True),
        ((4, 0, 3), True),
        ((4, 0, 6), True),
        # v4 boundary: first fixed version
        ((4, 0, 7), False),
        # v4 safe
        ((4, 1, 0), False),
        # None always safe (detection failed)
        (None, False),
    ],
    ids=[
        "v3_broken_lo",
        "v3_broken_mid",
        "v3_broken_hi",
        "v3_fixed_boundary",
        "v3_safe_below",
        "v3_6_safe",
        "v3_8_safe",
        "v4_broken_lo",
        "v4_broken_mid",
        "v4_broken_hi",
        "v4_fixed_boundary",
        "v4_1_safe",
        "none_detection_failed",
    ],
)
def test_argo_version_has_conditional_bug(version_tuple, expected):
    assert _argo_version_has_conditional_bug(version_tuple) == expected


# ---------------------------------------------------------------------------
# ArgoClient.get_server_version() — unit tests (no real cluster)
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal urllib response-like object."""

    def __init__(self, body):
        self._body = body.encode() if isinstance(body, str) else body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


def _make_client(mocker):
    """Return an ArgoClient whose KubernetesClient is fully mocked."""
    mocker.patch(
        "metaflow.plugins.argo.argo_client.KubernetesClient.__init__",
        return_value=None,
    )
    client = ArgoClient.__new__(ArgoClient)
    client._namespace = "default"
    client._group = "argoproj.io"
    client._version = "v1alpha1"
    client._client = mocker.MagicMock()
    return client


def test_get_server_version_from_rest_api(mocker):
    """get_server_version() returns the version from the Argo REST API."""
    client = _make_client(mocker)
    mocker.patch(
        "metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL",
        "https://argo.example.com",
    )
    fake_resp = _FakeResponse(json.dumps({"version": "v3.7.16", "gitTag": "v3.7.16"}))
    mocker.patch("urllib.request.urlopen", return_value=fake_resp)

    assert client.get_server_version() == "v3.7.16"


def test_get_server_version_rest_api_uses_git_tag_fallback(mocker):
    """Falls back to gitTag when 'version' key is absent."""
    client = _make_client(mocker)
    mocker.patch(
        "metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL",
        "https://argo.example.com",
    )
    fake_resp = _FakeResponse(json.dumps({"gitTag": "v3.7.18"}))
    mocker.patch("urllib.request.urlopen", return_value=fake_resp)

    assert client.get_server_version() == "v3.7.18"


def test_get_server_version_rest_api_failure_falls_through_to_deployment(mocker):
    """A failed REST call falls through to the Deployment strategy."""
    client = _make_client(mocker)
    mocker.patch(
        "metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL",
        "https://argo.example.com",
    )
    mocker.patch("urllib.request.urlopen", side_effect=OSError("connection refused"))
    mocker.patch("metaflow.plugins.argo.argo_client.KUBERNETES_NAMESPACE", "default")

    # Only the "argo" namespace has the deployment (common dedicated install).
    def _fake_read(name, namespace):
        if namespace == "argo":
            container = mocker.MagicMock()
            container.image = "quay.io/argoproj/workflow-controller:v3.7.11"
            deployment = mocker.MagicMock()
            deployment.spec.template.spec.containers = [container]
            return deployment
        raise Exception("not found")

    client._client.get().AppsV1Api().read_namespaced_deployment.side_effect = _fake_read

    assert client.get_server_version() == "v3.7.11"


@pytest.mark.parametrize(
    "image, expected",
    [
        ("quay.io/argoproj/workflow-controller:v3.7.16", "v3.7.16"),
        ("argoproj/workflow-controller:v4.0.7", "v4.0.7"),
        # no v-prefix
        ("myregistry/argo:3.7.11", "3.7.11"),
        # 'latest' tag should NOT be returned (not a semver)
        ("argoproj/workflow-controller:latest", None),
        # SHA digest should NOT be returned
        ("argoproj/workflow-controller@sha256:abc123def", None),
    ],
    ids=[
        "quay_v_prefix",
        "plain_argoproj_v4",
        "custom_registry_no_v",
        "latest_tag_rejected",
        "digest_rejected",
    ],
)
def test_get_server_version_from_deployment_image(mocker, image, expected):
    """Version is parsed from the controller Deployment's container image tag."""
    client = _make_client(mocker)
    mocker.patch("metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL", None)
    mocker.patch("metaflow.plugins.argo.argo_client.KUBERNETES_NAMESPACE", "argo")

    fake_container = mocker.MagicMock()
    fake_container.image = image
    fake_deployment = mocker.MagicMock()
    fake_deployment.spec.template.spec.containers = [fake_container]
    client._client.get().AppsV1Api().read_namespaced_deployment.return_value = (
        fake_deployment
    )

    assert client.get_server_version() == expected


def test_get_server_version_returns_none_when_both_fail(mocker):
    """Returns None when both the REST API and all Deployment lookups fail."""
    client = _make_client(mocker)
    mocker.patch("metaflow.plugins.argo.argo_client.ARGO_WORKFLOWS_UI_URL", None)
    mocker.patch("metaflow.plugins.argo.argo_client.KUBERNETES_NAMESPACE", "default")
    client._client.get().AppsV1Api().read_namespaced_deployment.side_effect = Exception(
        "not found"
    )

    assert client.get_server_version() is None


# ---------------------------------------------------------------------------
# deploy() version gate integration — all driven through get_server_version()
# ---------------------------------------------------------------------------


def _make_argo(mocker, flow_cls, name):
    mocker.patch.object(ArgoWorkflows, "_compile_workflow_template", return_value=None)
    mocker.patch.object(ArgoWorkflows, "_compile_sensor", return_value=None)
    return ArgoWorkflows(
        name=name,
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


@pytest.fixture
def conditional_argo(mocker):
    return _make_argo(mocker, ConditionalFlow, "conditional-flow")


@pytest.fixture
def linear_argo(mocker):
    return _make_argo(mocker, LinearFlow, "linear-flow")


@pytest.mark.parametrize(
    "detected",
    ["3.7.11", "3.7.13", "v3.7.15", "4.0.0", "v4.0.6"],
    ids=["v3_lo", "v3_mid", "v3_hi", "v4_lo", "v4_hi"],
)
def test_deploy_raises_for_broken_detected_version(mocker, conditional_argo, detected):
    """A conditional flow raises when a broken version is auto-detected."""
    mocker.patch.object(ArgoClient, "get_server_version", return_value=detected)
    mocker.patch.object(conditional_argo, "cleanup_previous_sensors")

    with pytest.raises(ArgoWorkflowsException, match="15932"):
        conditional_argo.deploy()


@pytest.mark.parametrize(
    "detected",
    ["3.6.0", "3.7.10", "3.7.16", "v3.7.18", "4.0.7", "v4.1.0", None],
    ids=[
        "v3_6_safe",
        "v3_7_10_safe",
        "v3_fixed_boundary",
        "v3_recent",
        "v4_fixed_boundary",
        "v4_1_safe",
        "detection_failed",
    ],
)
def test_deploy_proceeds_for_safe_or_unknown_version(
    mocker, conditional_argo, detected
):
    """Safe or undetected versions must not trigger the version gate."""
    mocker.patch.object(ArgoClient, "get_server_version", return_value=detected)
    mocker.patch.object(conditional_argo, "cleanup_previous_sensors")

    try:
        conditional_argo.deploy()
    except ArgoWorkflowsException as exc:
        assert "15932" not in str(
            exc
        ), "Gate should not fire for detected=%r but got: %s" % (detected, exc)
    except Exception:
        pass  # registration failure without a real cluster is expected


def test_deploy_linear_flow_skips_version_check(mocker, linear_argo):
    """A flow with no conditional steps never calls get_server_version()."""
    mock_gsv = mocker.patch.object(ArgoClient, "get_server_version")
    mocker.patch.object(linear_argo, "cleanup_previous_sensors")

    try:
        linear_argo.deploy()
    except Exception:
        pass

    mock_gsv.assert_not_called()


def test_deploy_detection_failure_does_not_block(mocker, conditional_argo):
    """When get_server_version() returns None the gate is skipped entirely."""
    mocker.patch.object(ArgoClient, "get_server_version", return_value=None)
    mocker.patch.object(conditional_argo, "cleanup_previous_sensors")

    try:
        conditional_argo.deploy()
    except ArgoWorkflowsException as exc:
        assert "15932" not in str(exc)
    except Exception:
        pass


def test_deploy_error_message_names_broken_version(mocker, conditional_argo):
    """The error message must include the detected version string."""
    mocker.patch.object(ArgoClient, "get_server_version", return_value="3.7.13")
    mocker.patch.object(conditional_argo, "cleanup_previous_sensors")

    with pytest.raises(ArgoWorkflowsException) as exc_info:
        conditional_argo.deploy()

    assert "3.7.13" in str(exc_info.value)


def test_deploy_error_message_names_conditional_steps(mocker, conditional_argo):
    """The error message must mention at least one conditional step name."""
    mocker.patch.object(ArgoClient, "get_server_version", return_value="3.7.11")
    mocker.patch.object(conditional_argo, "cleanup_previous_sensors")

    with pytest.raises(ArgoWorkflowsException) as exc_info:
        conditional_argo.deploy()

    msg = str(exc_info.value)
    assert any(step in msg for step in conditional_argo.conditional_nodes)
