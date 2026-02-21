"""
Tests for optional jobRoleArn behaviour in AWS Batch job definitions.

AWS Batch does not require jobRoleArn when the container can acquire
credentials from the compute environment's EC2 instance profile or ECS
task role.  These tests verify that:

  1. jobRoleArn is omitted from containerProperties when no role is given,
     so boto3 does not raise ParamValidationError on None.
  2. jobRoleArn is included when a role ARN is supplied.
  3. BatchJob.execute() no longer raises when iam_role is None.
"""

from unittest.mock import MagicMock

import pytest

from metaflow.plugins.aws.batch.batch_client import BatchJob

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_client(platform="EC2"):
    """Return a mock boto3 Batch client that satisfies _register_job_definition."""
    client = MagicMock()
    client.describe_job_queues.return_value = {
        "jobQueues": [
            {
                "computeEnvironmentOrder": [
                    {
                        "computeEnvironment": "arn:aws:batch:us-east-1:123:compute-env/test"
                    }
                ]
            }
        ]
    }
    client.describe_compute_environments.return_value = {
        "computeEnvironments": [{"computeResources": {"type": platform}}]
    }
    # Simulate no existing job definition so register_job_definition is called.
    client.describe_job_definitions.return_value = {"jobDefinitions": []}
    client.register_job_definition.return_value = {
        "jobDefinitionArn": "arn:aws:batch:us-east-1:123:job-definition/metaflow_abc:1"
    }
    return client


def _captured_job_definition(mock_client):
    """Return the dict passed to register_job_definition."""
    assert (
        mock_client.register_job_definition.called
    ), "register_job_definition was not called"
    _, kwargs = mock_client.register_job_definition.call_args
    return kwargs


# ---------------------------------------------------------------------------
# jobRoleArn tests
# ---------------------------------------------------------------------------


def test_job_role_arn_omitted_when_not_provided():
    """jobRoleArn must not appear in containerProperties when job_role is None."""
    client = _make_mock_client()
    job = BatchJob(client)

    job._register_job_definition(
        image="python:3.11",
        job_role=None,
        job_queue="my-queue",
        execution_role=None,
        shared_memory=None,
        max_swap=None,
        swappiness=None,
        inferentia=None,
        efa=None,
        memory=4096,
        host_volumes=None,
        efs_volumes=None,
        use_tmpfs=False,
        tmpfs_tempdir=True,
        tmpfs_size=None,
        tmpfs_path="/metaflow_temp",
        num_parallel=0,
        ephemeral_storage=None,
        log_driver=None,
        log_options=None,
        privileged=None,
    )

    job_def = _captured_job_definition(client)
    assert (
        "jobRoleArn" not in job_def["containerProperties"]
    ), "jobRoleArn should be absent when job_role is None"


def test_job_role_arn_included_when_provided():
    """jobRoleArn must be present in containerProperties when a role is given."""
    role = "arn:aws:iam::123456789012:role/MyBatchRole"
    client = _make_mock_client()
    job = BatchJob(client)

    job._register_job_definition(
        image="python:3.11",
        job_role=role,
        job_queue="my-queue",
        execution_role=None,
        shared_memory=None,
        max_swap=None,
        swappiness=None,
        inferentia=None,
        efa=None,
        memory=4096,
        host_volumes=None,
        efs_volumes=None,
        use_tmpfs=False,
        tmpfs_tempdir=True,
        tmpfs_size=None,
        tmpfs_path="/metaflow_temp",
        num_parallel=0,
        ephemeral_storage=None,
        log_driver=None,
        log_options=None,
        privileged=None,
    )

    job_def = _captured_job_definition(client)
    assert (
        job_def["containerProperties"].get("jobRoleArn") == role
    ), "jobRoleArn should equal the supplied role ARN"


# ---------------------------------------------------------------------------
# execute() guard tests
# ---------------------------------------------------------------------------


def test_execute_does_not_raise_without_iam_role():
    """execute() must not raise when iam_role is None (no role required)."""
    client = _make_mock_client()
    client.submit_job.return_value = {"jobId": "abc-123"}

    # describe_jobs needed by RunningJob.update()
    client.describe_jobs.return_value = {
        "jobs": [{"jobId": "abc-123", "status": "SUBMITTED"}]
    }

    job = BatchJob(client)
    job._image = "python:3.11"
    job._iam_role = None
    job._execution_role = None
    job._task_id = "task-1"
    job.payload["jobName"] = "test-job"
    job.payload["jobQueue"] = "my-queue"
    job.payload["jobDefinition"] = (
        "arn:aws:batch:us-east-1:123:job-definition/metaflow_abc:1"
    )
    job.payload["containerOverrides"]["command"] = ["echo", "hi"]

    # Should not raise
    job.execute()


def test_execute_still_raises_without_image():
    """execute() must still raise when no image is set (image is required)."""
    from metaflow.plugins.aws.batch.batch_client import BatchJobException

    job = BatchJob(MagicMock())
    job._image = None
    job._iam_role = None

    with pytest.raises(BatchJobException, match="No docker image specified"):
        job.execute()
