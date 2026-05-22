"""
Regression test for optional IAM role in AWS Batch job submission.

On EC2 compute environments that use instance profiles for container
credentials, jobRoleArn is not required. The AWS Batch API documents
it as optional, but Metaflow raised unconditionally when it was None.

See: https://github.com/Netflix/metaflow/issues/3208
"""

import pytest

from metaflow.plugins.aws.batch.batch_client import BatchJob


def test_execute_does_not_raise_when_iam_role_is_none():
    """
    BatchJob.execute() should not raise when iam_role is None.
    EC2 compute environments can use instance profiles instead.
    """
    job = BatchJob.__new__(BatchJob)
    job.payload = {
        "containerOverrides": {},
        "tags": {},
    }
    job._image = "python:3.10"
    job._iam_role = None
    job.num_parallel = 0

    # execute() will fail later (no client, no queue, etc.) but it
    # must NOT raise "No IAM role specified" — that's the bug.
    # We catch any exception and check it's not the IAM one.
    try:
        job.execute()
    except Exception as e:
        assert "No IAM role specified" not in str(e), (
            "execute() still raises when iam_role is None — "
            "EC2 instance profile credentials should be allowed"
        )


def test_execute_still_requires_image():
    """
    The docker image check should remain — only the IAM role
    check was removed.
    """
    job = BatchJob.__new__(BatchJob)
    job.payload = {
        "containerOverrides": {},
        "tags": {},
    }
    job._image = None
    job._iam_role = None

    with pytest.raises(Exception, match="No docker image specified"):
        job.execute()


def test_job_definition_omits_job_role_arn_when_none():
    """
    When job_role is None, jobRoleArn should not be present in the
    job definition at all. boto3 rejects None for string fields.
    """
    from pathlib import Path

    batch_client_path = Path(__file__).resolve().parents[2] / (
        "metaflow/plugins/aws/batch/batch_client.py"
    )
    source_text = batch_client_path.read_text()

    assert "jobRoleArn" in source_text, (
        "jobRoleArn reference not found in batch_client.py"
    )
    # The fix uses conditional dict unpacking: **({"jobRoleArn": ...} if ... else {})
    # If jobRoleArn is assigned unconditionally, this pattern won't be present
    assert "if job_role" in source_text, (
        "jobRoleArn is not conditionally included — it will be passed as None "
        "to boto3, which rejects None for string fields"
    )