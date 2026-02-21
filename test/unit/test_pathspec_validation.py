"""Tests for pathspec validation in MetaflowObject subclasses.

Verifies that Flow, Run, Step, Task, and DataArtifact reject pathspecs
with wrong component counts, trailing slashes, and empty segments.
See: https://github.com/Netflix/metaflow/issues/948
"""

import pytest

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
