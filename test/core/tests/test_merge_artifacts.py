"""merge_artifacts: happy path + error semantics."""

import pytest

from metaflow import FlowSpec, step


class MergeArtifactsHappyFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.left, self.right)

    @step
    def left(self):
        self.left_data = "L"
        self.shared = "agreed"
        self.next(self.join)

    @step
    def right(self):
        self.right_data = "R"
        self.shared = "agreed"
        self.next(self.join)

    @step
    def join(self, inputs):
        self.merge_artifacts(inputs)
        assert self.left_data == "L"
        assert self.right_data == "R"
        assert self.shared == "agreed"
        self.next(self.end)

    @step
    def end(self):
        pass


class MergeArtifactsConflictFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.left, self.right)

    @step
    def left(self):
        self.shared = "from-left"
        self.next(self.join)

    @step
    def right(self):
        self.shared = "from-right"
        self.next(self.join)

    @step
    def join(self, inputs):
        # Different values for `shared` on the two branches → conflict.
        self.merge_artifacts(inputs)
        self.next(self.end)

    @step
    def end(self):
        pass


class MergeArtifactsBadOptionsFlow(FlowSpec):
    @step
    def start(self):
        self.next(self.left, self.right)

    @step
    def left(self):
        self.left_data = "L"
        self.next(self.join)

    @step
    def right(self):
        self.right_data = "R"
        self.next(self.join)

    @step
    def join(self, inputs):
        # Passing both include and exclude is a usage error.
        self.merge_artifacts(inputs, include=["left_data"], exclude=["right_data"])
        self.next(self.end)

    @step
    def end(self):
        pass


def test_merge_artifacts_happy_path(metaflow_runner, executor):
    result = metaflow_runner(MergeArtifactsHappyFlow, executor=executor)
    assert result.successful, result.stderr
    join_data = result.run()["join"].task.data
    assert join_data.left_data == "L"
    assert join_data.right_data == "R"
    assert join_data.shared == "agreed"


def test_merge_artifacts_conflict_fails(metaflow_runner, executor):
    """Conflicting values for the same artifact across branches → run fails."""
    result = metaflow_runner(MergeArtifactsConflictFlow, executor=executor)
    assert not result.successful, "merge_artifacts should reject conflicting values"
    # On the cli executor, we capture stdout/stderr; verify the textual
    # hint there. The api executor doesn't read log files on the failure
    # path, so just trust the non-zero returncode.
    if executor == "cli":
        combined = result.stdout + result.stderr
        assert (
            "UnhandledInMergeArtifacts" in combined
            or "merge_artifacts" in combined.lower()
        ), combined


def test_merge_artifacts_include_and_exclude_fails(metaflow_runner, executor):
    """Passing both include= and exclude= is a usage error."""
    result = metaflow_runner(MergeArtifactsBadOptionsFlow, executor=executor)
    assert not result.successful, "include + exclude should be rejected"
