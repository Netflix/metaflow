"""Tests for pathspec validation in MetaflowObject subclasses.

Verifies that Flow, Run, Step, Task, and DataArtifact reject pathspecs
with wrong component counts, trailing slashes, and empty segments.
Also verifies that valid pathspecs with trailing slashes are accepted
after normalization (the core bug scenario from #948).
See: https://github.com/Netflix/metaflow/issues/948
"""

import pytest
from unittest.mock import patch

from metaflow.exception import MetaflowInvalidPathspec

# We test validation by calling the constructors with invalid pathspecs.
# The MetaflowInvalidPathspec should be raised *before* any metadata
# service call, so no mocking is needed for the invalid cases.


class TestFlowPathspec:
    def test_rejects_too_many_components(self):
        from metaflow import Flow

        with pytest.raises(MetaflowInvalidPathspec):
            Flow("MyFlow/123")

    def test_rejects_trailing_slash_extra_component(self):
        """Flow('MyFlow/123/') should still fail after trailing slash is stripped."""
        from metaflow import Flow

        # After stripping the trailing slash, 'MyFlow/123/' becomes
        # 'MyFlow/123' which has 2 components — still too many for Flow.
        with pytest.raises(MetaflowInvalidPathspec):
            Flow("MyFlow/123/")


class TestRunPathspec:
    def test_rejects_too_few_components(self):
        from metaflow import Run

        with pytest.raises(MetaflowInvalidPathspec):
            Run("MyFlow")

    def test_rejects_too_many_components(self):
        from metaflow import Run

        with pytest.raises(MetaflowInvalidPathspec):
            Run("MyFlow/123/start")

    def test_rejects_trailing_slash_extra_component(self):
        """Run('MyFlow/123/') used to silently create a 3-component
        pathspec that would pass the Step validation instead."""
        from metaflow import Run

        with pytest.raises(MetaflowInvalidPathspec):
            Run("MyFlow/123/start/")


class TestStepPathspec:
    def test_rejects_too_few_components(self):
        from metaflow import Step

        with pytest.raises(MetaflowInvalidPathspec):
            Step("MyFlow/123")

    def test_rejects_too_many_components(self):
        from metaflow import Step

        with pytest.raises(MetaflowInvalidPathspec):
            Step("MyFlow/123/start/456")

    def test_rejects_trailing_slash_extra_component(self):
        from metaflow import Step

        with pytest.raises(MetaflowInvalidPathspec):
            Step("MyFlow/123/start/456/")


class TestTaskPathspec:
    def test_rejects_too_few_components(self):
        from metaflow import Task

        with pytest.raises(MetaflowInvalidPathspec):
            Task("MyFlow/123/start")

    def test_rejects_too_many_components(self):
        from metaflow import Task

        with pytest.raises(MetaflowInvalidPathspec):
            Task("MyFlow/123/start/456/artifact")

    def test_rejects_trailing_slash_extra_component(self):
        from metaflow import Task

        with pytest.raises(MetaflowInvalidPathspec):
            Task("MyFlow/123/start/456/artifact/")


class TestDataArtifactPathspec:
    def test_rejects_too_few_components(self):
        from metaflow import DataArtifact

        with pytest.raises(MetaflowInvalidPathspec):
            DataArtifact("MyFlow/123/start/456")

    def test_rejects_too_many_components(self):
        from metaflow import DataArtifact

        with pytest.raises(MetaflowInvalidPathspec):
            DataArtifact("MyFlow/123/start/456/artifact/extra")

    def test_rejects_trailing_slash_extra_component(self):
        from metaflow import DataArtifact

        with pytest.raises(MetaflowInvalidPathspec):
            DataArtifact("MyFlow/123/start/456/artifact/extra/")


class TestEmptyComponents:
    def test_rejects_empty_segment_in_run(self):
        """Run('MyFlow//123') should fail due to empty component."""
        from metaflow import Run

        with pytest.raises(MetaflowInvalidPathspec, match="empty components"):
            Run("MyFlow//123")

    def test_rejects_empty_segment_in_step(self):
        from metaflow import Step

        with pytest.raises(MetaflowInvalidPathspec, match="empty components"):
            Step("MyFlow//start")

    def test_rejects_leading_slash(self):
        """Leading slash creates an empty first component."""
        from metaflow import Flow

        with pytest.raises(MetaflowInvalidPathspec, match="empty components"):
            Flow("/MyFlow")


# For positive tests (valid pathspecs that pass validation), we must mock
# the metadata service since _get_object is called after validation passes.
def _mock_metadata_object():
    """Minimal dict to satisfy MetaflowObject.__init__ after validation."""
    return {"ts_epoch": 1000, "system_tags": [], "tags": []}


class TestTrailingSlashAccepted:
    """Positive tests: valid pathspecs with trailing slashes are accepted
    after normalization. This is the core bug scenario from #948."""

    @patch("metaflow.client.core.get_namespace", return_value=None)
    @patch("metaflow.client.core.Metaflow")
    def test_flow_trailing_slash(self, mock_mf, mock_ns):
        from metaflow import Flow

        mock_mf.return_value.metadata.get_object.return_value = _mock_metadata_object()
        flow = Flow("MyFlow/")
        assert flow._pathspec == "MyFlow"
        assert flow.id == "MyFlow"

    @patch("metaflow.client.core.get_namespace", return_value=None)
    @patch("metaflow.client.core.Metaflow")
    def test_run_trailing_slash(self, mock_mf, mock_ns):
        from metaflow import Run

        mock_mf.return_value.metadata.get_object.return_value = _mock_metadata_object()
        run = Run("MyFlow/123/")
        assert run._pathspec == "MyFlow/123"
        assert run.id == "123"

    @patch("metaflow.client.core.get_namespace", return_value=None)
    @patch("metaflow.client.core.Metaflow")
    def test_step_trailing_slash(self, mock_mf, mock_ns):
        from metaflow import Step

        mock_mf.return_value.metadata.get_object.return_value = _mock_metadata_object()
        step = Step("MyFlow/123/start/")
        assert step._pathspec == "MyFlow/123/start"
        assert step.id == "start"

    @patch("metaflow.client.core.get_namespace", return_value=None)
    @patch("metaflow.client.core.Metaflow")
    def test_task_trailing_slash(self, mock_mf, mock_ns):
        from metaflow import Task

        mock_mf.return_value.metadata.get_object.return_value = _mock_metadata_object()
        task = Task("MyFlow/123/start/456/")
        assert task._pathspec == "MyFlow/123/start/456"
        assert task.id == "456"

    @patch("metaflow.client.core.get_namespace", return_value=None)
    @patch("metaflow.client.core.Metaflow")
    def test_data_artifact_trailing_slash(self, mock_mf, mock_ns):
        from metaflow import DataArtifact

        mock_mf.return_value.metadata.get_object.return_value = _mock_metadata_object()
        artifact = DataArtifact("MyFlow/123/start/456/x/")
        assert artifact._pathspec == "MyFlow/123/start/456/x"
        assert artifact.id == "x"
