import json
from unittest.mock import patch

import pytest

from metaflow.plugins.argo.argo_workflows_cli import sanitize_for_argo
from metaflow.plugins.argo.argo_workflows import ArgoWorkflowsException


@pytest.mark.parametrize(
    "name, expected",
    [
        ("a-valid-name", "a-valid-name"),
        ("removing---@+_characters@_+", "removing---characters"),
        ("numb3rs-4r3-0k-123", "numb3rs-4r3-0k-123"),
        ("proj3ct.br4nch.flow_name", "proj3ct.br4nch.flowname"),
        # should not break RFC 1123 subdomain requirements,
        # though trailing characters do not need to be sanitized due to a hash being appended to them.
        (
            "---1breaking1---.--2subdomain2--.-3rules3----",
            "1breaking1.2subdomain2.3rules3----",
        ),
        (
            "1brea---king1.2sub---domain2.-3ru-les3--",
            "1brea---king1.2sub---domain2.3ru-les3--",
        ),
        ("project.branch-cut-short-.flowname", "project.branch-cut-short.flowname"),
        ("test...name", "test.name"),
    ],
)
def test_sanitize_for_argo(name, expected):
    sanitized = sanitize_for_argo(name)
    assert sanitized == expected


class TestArgoWorkflowsLabels:
    """Tests for issue #2780: Allow Argo label customization."""

    def _make_argo_obj_with_labels(self, env_labels="", cli_labels=None):
        """Create a minimal ArgoWorkflows-like object to test _base_kubernetes_labels."""
        from metaflow.plugins.argo.argo_workflows import ArgoWorkflows

        # We only need to test _base_kubernetes_labels, so we create a mock
        # that has just the cli_labels attribute set.
        class FakeArgo:
            pass

        obj = FakeArgo()
        obj.cli_labels = cli_labels
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS", env_labels
        ):
            return ArgoWorkflows._base_kubernetes_labels(obj)

    def test_default_labels(self):
        """Without any custom labels, only system labels are present."""
        labels = self._make_argo_obj_with_labels()
        assert labels == {"app.kubernetes.io/part-of": "metaflow"}

    def test_env_labels(self):
        """METAFLOW_ARGO_WORKFLOWS_LABELS config adds custom labels."""
        labels = self._make_argo_obj_with_labels(
            env_labels='{"team": "ml", "env": "prod"}'
        )
        assert labels["team"] == "ml"
        assert labels["env"] == "prod"
        assert labels["app.kubernetes.io/part-of"] == "metaflow"

    def test_cli_labels(self):
        """--label CLI options add custom labels."""
        labels = self._make_argo_obj_with_labels(cli_labels=["team=ml", "env=prod"])
        assert labels["team"] == "ml"
        assert labels["env"] == "prod"
        assert labels["app.kubernetes.io/part-of"] == "metaflow"

    def test_cli_labels_override_env(self):
        """CLI --label takes precedence over METAFLOW_ARGO_WORKFLOWS_LABELS."""
        labels = self._make_argo_obj_with_labels(
            env_labels='{"team": "old-team"}', cli_labels=["team=new-team"]
        )
        assert labels["team"] == "new-team"

    def test_system_labels_cannot_be_overridden(self):
        """System labels always take precedence over user labels."""
        labels = self._make_argo_obj_with_labels(
            env_labels='{"app.kubernetes.io/part-of": "custom"}'
        )
        assert labels["app.kubernetes.io/part-of"] == "metaflow"

    def test_invalid_env_labels_json(self):
        """Invalid JSON in METAFLOW_ARGO_WORKFLOWS_LABELS raises an error."""
        with pytest.raises(ArgoWorkflowsException, match="valid JSON"):
            self._make_argo_obj_with_labels(env_labels="not-json")

    def test_invalid_env_labels_type(self):
        """Non-dict JSON in METAFLOW_ARGO_WORKFLOWS_LABELS raises an error."""
        with pytest.raises(ArgoWorkflowsException, match="JSON object"):
            self._make_argo_obj_with_labels(env_labels='["a", "b"]')

    def test_invalid_cli_label_format(self):
        """CLI --label without = raises an error."""
        with pytest.raises(ArgoWorkflowsException, match="key=value"):
            self._make_argo_obj_with_labels(cli_labels=["noequalssign"])

    def test_cli_label_with_equals_in_value(self):
        """CLI --label with = in value should work correctly."""
        labels = self._make_argo_obj_with_labels(cli_labels=["key=val=ue"])
        assert labels["key"] == "val=ue"
