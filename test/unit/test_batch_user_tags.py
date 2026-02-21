"""
Tests verifying that user-defined AWS Batch tags (aws_batch_tags /
METAFLOW_BATCH_DEFAULT_TAGS) are applied unconditionally, independent of the
METAFLOW_BATCH_EMIT_TAGS flag.

Previously, aws_batch_tags was applied only inside the `if BATCH_EMIT_TAGS:`
block, which forced users who wanted simple cost-attribution tags to also enable
all Metaflow internal metadata tags (requiring Batch:TagResource permissions
across the board).  After this change, user-defined tags are always applied when
set, while Metaflow's internal metadata tags remain gated on BATCH_EMIT_TAGS.
"""

from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_chained_batch_job():
    """
    Return a mock that supports method chaining (.tag(), .job_name(), etc.)
    and records tag() calls.
    """
    job = MagicMock()
    for method in [
        "job_name",
        "job_queue",
        "command",
        "image",
        "iam_role",
        "execution_role",
        "cpu",
        "gpu",
        "memory",
        "shared_memory",
        "max_swap",
        "swappiness",
        "inferentia",
        "efa",
        "timeout_in_secs",
        "job_def",
        "environment_variable",
        "parameter",
        "tag",
        "attempts",
        "task_id",
        "offload_command_to_s3",
        "privileged",
    ]:
        getattr(job, method).return_value = job
    return job


def _make_batch_instance():
    """Return a Batch instance with a mocked client."""
    from metaflow.plugins.aws.batch.batch import Batch

    mock_client = MagicMock()
    mock_job = _make_chained_batch_job()
    mock_client.job.return_value = mock_job

    env = MagicMock()
    env.get_environment_info.return_value = {}

    b = Batch.__new__(Batch)
    b._client = mock_client
    b.environment = env
    return b, mock_job


MINIMAL_ATTRS = {
    "metaflow.user": "test-user",
    "metaflow.flow_name": "TestFlow",
    "metaflow.run_id": "1",
    "metaflow.step_name": "start",
    "metaflow.task_id": "1",
    "metaflow.retry_count": "0",
}


def _call_create_job(b, aws_batch_tags, attrs=None, emit_tags=False):
    """
    Call create_job with minimal required args, patching _command and
    BATCH_EMIT_TAGS to isolate the tagging logic under test.
    """
    merged_attrs = {**MINIMAL_ATTRS, **(attrs or {})}
    with patch.object(b, "_command", return_value=["echo", "hello"]), patch(
        "metaflow.plugins.aws.batch.batch.BATCH_EMIT_TAGS", emit_tags
    ):
        b.create_job(
            step_name="start",
            step_cli="python flow.py",
            task_spec={},
            code_package_metadata="",
            code_package_sha="abc",
            code_package_url="s3://bucket/pkg.tgz",
            code_package_ds=MagicMock(),
            image="python:3.11",
            queue="my-queue",
            attrs=merged_attrs,
            aws_batch_tags=aws_batch_tags,
        )


def _tag_calls(mock_job):
    """Return list of (key, value) tuples from all job.tag() calls."""
    return [c.args for c in mock_job.tag.call_args_list]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_user_tags_applied_when_emit_tags_disabled():
    """aws_batch_tags must be applied even when BATCH_EMIT_TAGS=False."""
    b, mock_job = _make_batch_instance()

    _call_create_job(b, aws_batch_tags={"CostCenter": "12345", "Team": "ml-platform"})

    tags = _tag_calls(mock_job)
    assert ("CostCenter", "12345") in tags
    assert ("Team", "ml-platform") in tags


def test_no_metaflow_internal_tags_when_emit_tags_disabled():
    """
    Internal Metaflow tags (app, metaflow.flow_name …) must NOT appear
    when BATCH_EMIT_TAGS=False, even when aws_batch_tags is set.
    """
    b, mock_job = _make_batch_instance()

    _call_create_job(
        b,
        aws_batch_tags={"CostCenter": "12345"},
        attrs={"metaflow.flow_name": "MyFlow"},
    )

    tag_keys = [k for k, _v in _tag_calls(mock_job)]
    assert "app" not in tag_keys
    assert "metaflow.flow_name" not in tag_keys


def test_both_internal_and_user_tags_when_emit_tags_enabled():
    """
    When BATCH_EMIT_TAGS=True, both internal Metaflow tags and user-defined
    tags must be applied.
    """
    b, mock_job = _make_batch_instance()

    _call_create_job(
        b,
        aws_batch_tags={"CostCenter": "12345"},
        attrs={"metaflow.flow_name": "MyFlow"},
        emit_tags=True,
    )

    tags = _tag_calls(mock_job)
    tag_keys = [k for k, _v in tags]
    assert "app" in tag_keys
    assert ("CostCenter", "12345") in tags


def test_no_tags_applied_when_aws_batch_tags_is_none():
    """No tags at all when aws_batch_tags is None and BATCH_EMIT_TAGS=False."""
    b, mock_job = _make_batch_instance()

    _call_create_job(b, aws_batch_tags=None)

    mock_job.tag.assert_not_called()
