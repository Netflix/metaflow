import pytest
from unittest.mock import patch

from metaflow.plugins.kubernetes.kube_utils import KubernetesException


class TestArgoWorkflowsLabels:
    """Test _base_argo_labels() method with mocked configuration."""

    def _call_base_argo_labels(self):
        """
        Call _base_argo_labels without full ArgoWorkflows instantiation.

        The methods only use module-level config, not instance state, so we can
        call them on a minimal object with the methods bound to it.
        """
        from metaflow.plugins.argo.argo_workflows import ArgoWorkflows

        # Create a minimal object with both methods bound
        class MinimalArgo:
            _base_kubernetes_labels = ArgoWorkflows._base_kubernetes_labels
            _base_argo_labels = ArgoWorkflows._base_argo_labels

        obj = MinimalArgo()
        return obj._base_argo_labels()

    def test_default_labels_when_env_not_set(self):
        """Should return default labels when ARGO_WORKFLOWS_LABELS is empty."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS", ""
        ):
            labels = self._call_base_argo_labels()

        assert labels == {"app.kubernetes.io/part-of": "metaflow"}

    def test_adds_custom_labels_from_env(self):
        """Should add custom labels from ARGO_WORKFLOWS_LABELS."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            "team=ml,env=prod",
        ):
            labels = self._call_base_argo_labels()

        assert labels == {
            "app.kubernetes.io/part-of": "metaflow",
            "team": "ml",
            "env": "prod",
        }

    def test_custom_labels_override_defaults(self):
        """Custom labels should override default labels."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            "app.kubernetes.io/part-of=custom-app",
        ):
            labels = self._call_base_argo_labels()

        assert labels == {"app.kubernetes.io/part-of": "custom-app"}

    def test_single_label(self):
        """Should handle a single label correctly."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            "cost-center=12345",
        ):
            labels = self._call_base_argo_labels()

        assert labels == {
            "app.kubernetes.io/part-of": "metaflow",
            "cost-center": "12345",
        }

    def test_invalid_label_value_raises_exception(self):
        """Should raise exception for invalid label values."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            "team=invalid value with spaces",
        ):
            with pytest.raises(KubernetesException):
                self._call_base_argo_labels()

    def test_label_value_too_long_raises_exception(self):
        """Should raise exception for label values exceeding 63 chars."""
        long_value = "a" * 64
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            f"team={long_value}",
        ):
            with pytest.raises(KubernetesException):
                self._call_base_argo_labels()
