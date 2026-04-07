"""
Tests for pathspec validation in the Metaflow Client API.

GitHub Issue: https://github.com/Netflix/metaflow/issues/948
"Pathspec to create Flow/Run/Step/Task/DataArtifact is not validated"

Each test is annotated with:
 # fails before fix, passes after fix — for cases that must now raise
 # passes before fix, passes after fix — for valid-format cases (regression guard)

Valid-format pathspecs still raise MetaflowNotFound because there is no real
metadata backend running during unit tests. That is expected and correct; the
key assertion is that MetaflowInvalidPathspec is NOT raised for valid formats.
"""

import pytest

from metaflow import DataArtifact, Flow, Run, Step, Task
from metaflow.exception import MetaflowInvalidPathspec


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _raises_invalid_pathspec(cls, pathspec, **kwargs):
    """Assert that constructing cls(pathspec) raises MetaflowInvalidPathspec."""
    with pytest.raises(MetaflowInvalidPathspec):
        cls(pathspec, **kwargs)


def _raises_not_invalid_pathspec(cls, pathspec, **kwargs):
    """
    Assert that constructing cls(pathspec) does NOT raise MetaflowInvalidPathspec.

    It may raise MetaflowNotFound (no backend) or succeed — both are fine.
    """
    try:
        cls(pathspec, **kwargs)
    except MetaflowInvalidPathspec:
        pytest.fail(
            "%s(%r) raised MetaflowInvalidPathspec but should not have."
            % (cls.__name__, pathspec)
        )
    except Exception:
        # Any other exception (e.g. MetaflowNotFound, connection error) is OK.
        pass


# ===========================================================================
# Flow
# ===========================================================================


# fails before fix, passes after fix
def test_flow_none_pathspec_raises():
    """Flow(None) must raise MetaflowInvalidPathspec."""
    _raises_invalid_pathspec(Flow, None)


# fails before fix, passes after fix
def test_flow_empty_string_raises():
    """Flow('') must raise MetaflowInvalidPathspec."""
    _raises_invalid_pathspec(Flow, "")


# fails before fix, passes after fix
def test_flow_too_many_parts_raises():
    """Flow('MyFlow/1234') is wrong — Flow takes exactly 1 part."""
    _raises_invalid_pathspec(Flow, "MyFlow/1234")


# fails before fix, passes after fix
def test_flow_empty_segment_raises():
    """Flow('') or a slash-only pathspec must raise."""
    _raises_invalid_pathspec(Flow, "/")


# passes before fix, passes after fix
def test_flow_valid_pathspec():
    """Flow('MyFlow') is the correct format — validation must not raise."""
    _raises_not_invalid_pathspec(Flow, "MyFlow")


# passes before fix, passes after fix
def test_flow_trailing_slash_treated_as_valid():
    """
    Flow('MyFlow/') should be normalised to 'MyFlow' (1 part) and not raise
    MetaflowInvalidPathspec; it may raise MetaflowNotFound afterward.
    """
    _raises_not_invalid_pathspec(Flow, "MyFlow/")


# ===========================================================================
# Run
# ===========================================================================


# fails before fix, passes after fix
def test_run_none_pathspec_raises():
    """Run(None) must raise MetaflowInvalidPathspec."""
    _raises_invalid_pathspec(Run, None)


# fails before fix, passes after fix
def test_run_empty_string_raises():
    """Run('') must raise MetaflowInvalidPathspec."""
    _raises_invalid_pathspec(Run, "")


# fails before fix, passes after fix
def test_run_too_few_parts_raises():
    """Run('MyFlow') is wrong — Run takes exactly 2 parts."""
    _raises_invalid_pathspec(Run, "MyFlow")


# fails before fix, passes after fix
def test_run_too_many_parts_raises():
    """Run('MyFlow/1234/extra') is wrong — Run takes exactly 2 parts."""
    _raises_invalid_pathspec(Run, "MyFlow/1234/extra")


# fails before fix, passes after fix
def test_run_empty_segment_raises():
    """Run('MyFlow//1234') contains an empty segment and must raise."""
    _raises_invalid_pathspec(Run, "MyFlow//1234")


# fails before fix, passes after fix — trailing slash strips to 3 parts (step shape)
def test_run_with_step_like_path_fails_after_normalize():
    """Run('HelloFlow/1/start/') normalises to 3 parts; Run requires 2."""
    _raises_invalid_pathspec(Run, "HelloFlow/1/start/")


# passes before fix, passes after fix
def test_run_valid_pathspec():
    """Run('MyFlow/1234') is the correct format — validation must not raise."""
    _raises_not_invalid_pathspec(Run, "MyFlow/1234")


# ===========================================================================
# Step
# ===========================================================================


# fails before fix, passes after fix
def test_step_none_pathspec_raises():
    """Step(None) must raise MetaflowInvalidPathspec."""
    _raises_invalid_pathspec(Step, None)


# fails before fix, passes after fix
def test_step_too_few_parts_raises():
    """Step('MyFlow/1234') is wrong — Step takes exactly 3 parts."""
    _raises_invalid_pathspec(Step, "MyFlow/1234")


# fails before fix, passes after fix
def test_step_too_many_parts_raises():
    """Step('MyFlow/1234/my_step/extra') is wrong — Step takes 3 parts."""
    _raises_invalid_pathspec(Step, "MyFlow/1234/my_step/extra")


# fails before fix, passes after fix
def test_step_empty_segment_raises():
    """Step('MyFlow//1234/my_step') contains an empty segment."""
    _raises_invalid_pathspec(Step, "MyFlow//1234/my_step")


# passes before fix, passes after fix
def test_step_valid_pathspec():
    """Step('MyFlow/1234/my_step') is the correct format."""
    _raises_not_invalid_pathspec(Step, "MyFlow/1234/my_step")


# ===========================================================================
# Task (the motivating example from the issue)
# ===========================================================================


# fails before fix, passes after fix
def test_task_none_pathspec_raises():
    """Task(None) must raise MetaflowInvalidPathspec."""
    _raises_invalid_pathspec(Task, None)


# fails before fix, passes after fix
def test_task_empty_string_raises():
    """Task('') must raise MetaflowInvalidPathspec."""
    _raises_invalid_pathspec(Task, "")


# fails before fix, passes after fix
def test_task_too_few_parts_raises():
    """
    Task('MyFlow/1234/my_step') is the exact bug from issue #948.
    Only 3 parts — Task requires 4.
    """
    _raises_invalid_pathspec(Task, "MyFlow/1234/my_step")


# fails before fix, passes after fix
def test_task_too_many_parts_raises():
    """Task('MyFlow/1234/my_step/56789/extra') has 5 parts — too many."""
    _raises_invalid_pathspec(Task, "MyFlow/1234/my_step/56789/extra")


# fails before fix, passes after fix
def test_task_empty_segment_raises():
    """Task('MyFlow//1234/my_step/56789') contains an empty segment."""
    _raises_invalid_pathspec(Task, "MyFlow//1234/my_step/56789")


# fails before fix, passes after fix
def test_task_trailing_slash_wrong_count_raises():
    """
    Task('MyFlow/1234/my_step/') — after stripping the trailing slash this
    becomes 'MyFlow/1234/my_step' which is only 3 parts, so it must raise.
    """
    _raises_invalid_pathspec(Task, "MyFlow/1234/my_step/")


# passes before fix, passes after fix
def test_task_valid_pathspec():
    """
    Task('MyFlow/1234/my_step/56789') is the correct 4-part format.
    Validation must not raise MetaflowInvalidPathspec.
    (May raise MetaflowNotFound because no real metadata backend is running.)
    """
    _raises_not_invalid_pathspec(Task, "MyFlow/1234/my_step/56789")


# passes before fix, passes after fix
def test_task_trailing_slash_valid_normalised():
    """
    Task('MyFlow/1234/my_step/56789/') — trailing slash is stripped,
    leaving 4 valid parts. Must not raise MetaflowInvalidPathspec.
    """
    _raises_not_invalid_pathspec(Task, "MyFlow/1234/my_step/56789/")


# ===========================================================================
# DataArtifact
# ===========================================================================


# fails before fix, passes after fix
def test_artifact_none_pathspec_raises():
    """DataArtifact(None) must raise MetaflowInvalidPathspec."""
    _raises_invalid_pathspec(DataArtifact, None)


# fails before fix, passes after fix
def test_artifact_too_few_parts_raises():
    """DataArtifact('MyFlow/1234/my_step/56789') has only 4 parts — too few."""
    _raises_invalid_pathspec(DataArtifact, "MyFlow/1234/my_step/56789")


# fails before fix, passes after fix
def test_artifact_too_many_parts_raises():
    """DataArtifact with 6 parts must raise."""
    _raises_invalid_pathspec(
        DataArtifact, "MyFlow/1234/my_step/56789/my_artifact/extra"
    )


# fails before fix, passes after fix
def test_artifact_empty_segment_raises():
    """DataArtifact with an empty segment must raise."""
    _raises_invalid_pathspec(DataArtifact, "MyFlow//1234/my_step/56789/my_artifact")


# passes before fix, passes after fix
def test_artifact_valid_pathspec():
    """DataArtifact('MyFlow/1234/my_step/56789/my_artifact') is valid."""
    _raises_not_invalid_pathspec(DataArtifact, "MyFlow/1234/my_step/56789/my_artifact")


# ===========================================================================
# Error-message content (spot-check)
# ===========================================================================


def test_error_message_includes_pathspec():
    """The error message must include the bad pathspec string."""
    with pytest.raises(MetaflowInvalidPathspec) as exc_info:
        Task("MyFlow/1234/my_step")
    assert "MyFlow/1234/my_step" in str(exc_info.value)


def test_error_message_includes_class_name():
    """The error message must mention the class name (Task)."""
    with pytest.raises(MetaflowInvalidPathspec) as exc_info:
        Task("MyFlow/1234/my_step")
    assert "Task" in str(exc_info.value)


def test_error_message_includes_expected_format():
    """The error message must include the expected format."""
    with pytest.raises(MetaflowInvalidPathspec) as exc_info:
        Task("MyFlow/1234/my_step")
    msg = str(exc_info.value)
    assert "FlowName/RunID/StepName/TaskID" in msg


def test_error_message_includes_part_counts():
    """The error message must mention both expected and actual part counts."""
    with pytest.raises(MetaflowInvalidPathspec) as exc_info:
        Task("MyFlow/1234/my_step")
    msg = str(exc_info.value)
    assert "4" in msg  # expected parts
    assert "3" in msg  # actual parts
