import os
import unittest
from unittest.mock import patch

from metaflow.plugins.argo.argo_workflows import ArgoWorkflows


class TestArgoWorkflowsLabels(unittest.TestCase):
    def test_base_kubernetes_labels_default(self):
        # By default we should at least set the part-of label.
        wf = object.__new__(ArgoWorkflows)
        with patch.dict(os.environ, {}, clear=False):
            labels = wf._base_kubernetes_labels()
        self.assertEqual(labels.get("app.kubernetes.io/part-of"), "metaflow")

    def test_base_kubernetes_labels_from_env(self):
        wf = object.__new__(ArgoWorkflows)
        with patch.dict(
            os.environ,
            {"METAFLOW_ARGO_WORKFLOWS_LABELS": "team=data,env=prod"},
            clear=False,
        ):
            labels = wf._base_kubernetes_labels()

        # Base label is preserved.
        self.assertEqual(labels.get("app.kubernetes.io/part-of"), "metaflow")
        # Custom labels from env are added.
        self.assertEqual(labels.get("team"), "data")
        self.assertEqual(labels.get("env"), "prod")
