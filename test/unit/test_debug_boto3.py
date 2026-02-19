import subprocess
import sys
import os
import pytest

try:
    import boto3

    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


@pytest.mark.skipif(not HAS_BOTO3, reason="boto3 not installed")
def test_boto3_debug_logging_via_subprocess():
    """Test that setting METAFLOW_DEBUG_BOTO3=1 prints boto3 session info."""
    # This script imports and triggers the boto3 client provider directly.
    script = "from metaflow.plugins.aws.aws_client import get_aws_client; get_aws_client('s3')"

    # Run with DEBUG DISABLED (default)
    env_disabled = os.environ.copy()
    # Ensure it's not set
    if "METAFLOW_DEBUG_BOTO3" in env_disabled:
        del env_disabled["METAFLOW_DEBUG_BOTO3"]

    try:
        out_disabled = subprocess.check_output(
            [sys.executable, "-c", script],
            stderr=subprocess.STDOUT,
            env=env_disabled,
            text=True,
        )
        assert "debug[boto3" not in out_disabled
    except subprocess.CalledProcessError as e:
        # If it fails (e.g., no AWS credentials), we can still inspect output
        assert "debug[boto3" not in e.output

    # Run with DEBUG ENABLED
    env_enabled = os.environ.copy()
    env_enabled["METAFLOW_DEBUG_BOTO3"] = "1"

    try:
        out_enabled = subprocess.check_output(
            [sys.executable, "-c", script],
            stderr=subprocess.STDOUT,
            env=env_enabled,
            text=True,
        )
        assert "debug[boto3" in out_enabled
        assert "Boto3 session created" in out_enabled
    except subprocess.CalledProcessError as e:
        # If it fails due to lack of credentials to get a client,
        # the session creation log should STILL be present.
        assert "debug[boto3" in e.output
        assert "Boto3 session created" in e.output
