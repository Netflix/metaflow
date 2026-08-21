import pytest
from unittest.mock import patch, MagicMock, PropertyMock

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
            _custom_argo_labels = ArgoWorkflows._custom_argo_labels
            _base_argo_labels = ArgoWorkflows._base_argo_labels

        obj = MinimalArgo()
        return obj._base_argo_labels()

    def test_default_labels_when_env_not_set(self):
        """Should return default labels when ARGO_WORKFLOWS_LABELS is empty."""
        with patch("metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS", ""):
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

    def test_custom_labels_do_not_override_internal_labels(self):
        """Custom labels must not override internal/base labels."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            "app.kubernetes.io/part-of=custom-app",
        ):
            labels = self._call_base_argo_labels()

        assert labels == {"app.kubernetes.io/part-of": "metaflow"}

    def test_custom_labels_alongside_part_of_override_attempt(self):
        """Unrelated custom labels pass through even when part-of override is attempted."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            "app.kubernetes.io/part-of=custom-app,team=ml",
        ):
            labels = self._call_base_argo_labels()

        assert labels == {
            "app.kubernetes.io/part-of": "metaflow",
            "team": "ml",
        }

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

    def test_invalid_label_key_empty_name_raises_exception(self):
        """Should raise exception for a key with an empty name segment (e.g. '=ml')."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            "=ml",
        ):
            with pytest.raises(KubernetesException):
                self._call_base_argo_labels()

    def test_invalid_label_key_with_spaces_raises_exception(self):
        """Should raise exception for a key containing spaces."""
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            " team=ml",
        ):
            with pytest.raises(KubernetesException):
                self._call_base_argo_labels()

    def test_invalid_label_key_too_long_raises_exception(self):
        """Should raise exception for a key name segment exceeding 63 chars."""
        long_key = "a" * 64
        with patch(
            "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
            f"{long_key}=ml",
        ):
            with pytest.raises(KubernetesException):
                self._call_base_argo_labels()


class TestArgoWorkflowsTemplateLabels:
    """Test that labels appear in compiled WorkflowTemplate and Sensor."""

    @pytest.fixture
    def mock_argo_workflows(self):
        """Create an ArgoWorkflows instance with mocked dependencies."""
        # Mock all the complex dependencies
        patches = [
            patch(
                "metaflow.plugins.argo.argo_workflows.ARGO_WORKFLOWS_LABELS",
                "team=ml-platform,env=production",
            ),
            patch(
                "metaflow.plugins.argo.argo_workflows.KUBERNETES_NAMESPACE", "test-ns"
            ),
            patch("metaflow.plugins.argo.argo_workflows.ARGO_EVENTS_EVENT", None),
            patch(
                "metaflow.plugins.argo.argo_workflows.ARGO_EVENTS_EVENT_SOURCE", None
            ),
            patch(
                "metaflow.plugins.argo.argo_workflows.ARGO_EVENTS_SERVICE_ACCOUNT", None
            ),
        ]

        for p in patches:
            p.start()

        from metaflow.plugins.argo.argo_workflows import ArgoWorkflows

        # Create mock graph with minimal structure
        mock_node = MagicMock()
        mock_node.name = "start"
        mock_node.type = "linear"
        mock_node.out_funcs = ["end"]
        mock_node.is_inside_foreach = False
        mock_node.parallel_foreach = False

        mock_end_node = MagicMock()
        mock_end_node.name = "end"
        mock_end_node.type = "end"
        mock_end_node.out_funcs = []
        mock_end_node.is_inside_foreach = False
        mock_end_node.parallel_foreach = False

        mock_graph = MagicMock()
        mock_graph.nodes = {"start": mock_node, "end": mock_end_node}
        mock_graph.__iter__ = lambda self: iter([mock_node, mock_end_node])

        # Create mock flow
        mock_flow = MagicMock()
        mock_flow.name = "TestFlow"
        mock_flow._flow_decorators = {}
        type(mock_flow)._parameters = PropertyMock(return_value={})
        type(mock_flow)._configs = PropertyMock(return_value={})

        # Create mock environment
        mock_environment = MagicMock()
        mock_environment.get_package_commands.return_value = []
        mock_environment.bootstrap_commands.return_value = []

        # Create mock datastore
        mock_datastore = MagicMock()
        mock_datastore.TYPE = "s3"

        # Create the instance - we'll patch the complex compilation methods
        with patch.object(ArgoWorkflows, "_compile_workflow_template"), patch.object(
            ArgoWorkflows, "_compile_sensor"
        ), patch.object(
            ArgoWorkflows, "_process_parameters", return_value=[]
        ), patch.object(
            ArgoWorkflows, "_process_config_parameters", return_value=[]
        ), patch.object(
            ArgoWorkflows, "_process_triggers", return_value=([], {})
        ), patch.object(
            ArgoWorkflows, "_get_schedule", return_value=(None, None)
        ), patch.object(
            ArgoWorkflows, "_parse_conditional_branches"
        ):
            argo = ArgoWorkflows(
                name="test-flow",
                graph=mock_graph,
                flow=mock_flow,
                code_package_metadata={},
                code_package_sha="abc123",
                code_package_url="s3://bucket/code.tar.gz",
                production_token="prod-token",
                metadata=MagicMock(),
                flow_datastore=mock_datastore,
                environment=mock_environment,
                event_logger=MagicMock(),
                monitor=MagicMock(),
                username="testuser",
            )

        for p in patches:
            p.stop()

        return argo

    def test_workflow_labels_includes_custom_labels(self, mock_argo_workflows):
        """Verify _workflow_labels (used at WorkflowTemplate/Workflow level) contains custom labels from env var."""
        labels = mock_argo_workflows._workflow_labels

        assert labels["app.kubernetes.io/part-of"] == "metaflow"
        assert labels["team"] == "ml-platform"
        assert labels["env"] == "production"

    def test_base_labels_excludes_custom_labels(self, mock_argo_workflows):
        """Verify _base_labels (used for pods/JobSet/Sensor) does NOT contain custom labels from env var."""
        labels = mock_argo_workflows._base_labels

        assert labels == {"app.kubernetes.io/part-of": "metaflow"}
        assert "team" not in labels
        assert "env" not in labels
