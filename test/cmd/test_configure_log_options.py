"""
Tests for METAFLOW_BATCH_LOG_OPTIONS configuration in configure_cmd.py

These tests verify the validation and parsing logic for CloudWatch logging
options when configuring AWS Batch through the CLI.
"""

from unittest.mock import patch
from metaflow.cmd.configure_cmd import configure_aws_batch


MOCK_PROMPT_VALUES = [
    "test-queue",  # METAFLOW_BATCH_JOB_QUEUE
    "arn:aws:iam::123456789012:role/test-role",  # METAFLOW_ECS_S3_ACCESS_IAM_ROLE
    "",  # METAFLOW_BATCH_CONTAINER_REGISTRY
    "",  # METAFLOW_BATCH_CONTAINER_IMAGE
]


def test_accepts_single_key_value_pair():
    """Test that a single log option in key:value format is accepted."""
    # Setup: Provide a valid single key:value option
    existing_env = {}

    with patch("metaflow.cmd.configure_cmd.click.prompt") as mock_prompt, patch(
        "metaflow.cmd.configure_cmd.click.confirm"
    ) as mock_confirm:
        # Mock Step Functions configuration prompt to skip it
        mock_confirm.return_value = False

        # Mock all the prompts required by configure_aws_batch
        mock_prompt.side_effect = [
            *MOCK_PROMPT_VALUES,
            "awslogs-group:aws/batch/jobs",  # METAFLOW_BATCH_LOG_OPTIONS - valid format
        ]

        result = configure_aws_batch(existing_env)

        # Assert: The option should be parsed and stored correctly
        assert result["METAFLOW_BATCH_LOG_OPTIONS"] == ["awslogs-group:aws/batch/jobs"]


def test_accepts_multiple_comma_separated_key_value_pairs():
    """Test that multiple comma-separated log options in key:value format are accepted."""
    # Setup: Provide multiple comma-separated key:value options
    existing_env = {}

    with patch("metaflow.cmd.configure_cmd.click.prompt") as mock_prompt, patch(
        "metaflow.cmd.configure_cmd.click.confirm"
    ) as mock_confirm:
        mock_confirm.return_value = False

        mock_prompt.side_effect = [
            *MOCK_PROMPT_VALUES,
            "awslogs-group:aws/batch/jobs,awslogs-stream-prefix:metaflow",  # Multiple valid options
        ]

        result = configure_aws_batch(existing_env)

        # Assert: All options should be parsed correctly
        assert result["METAFLOW_BATCH_LOG_OPTIONS"] == [
            "awslogs-group:aws/batch/jobs",
            "awslogs-stream-prefix:metaflow",
        ]


def test_rejects_option_without_colon_separator():
    """Test that log options missing the colon separator are rejected with error."""
    # Setup: Provide an invalid option without colon, then a valid one on retry
    existing_env = {}

    with patch("metaflow.cmd.configure_cmd.click.prompt") as mock_prompt, patch(
        "metaflow.cmd.configure_cmd.click.confirm"
    ) as mock_confirm, patch("metaflow.cmd.configure_cmd.echo") as mock_echo:
        mock_confirm.return_value = False

        mock_prompt.side_effect = [
            *MOCK_PROMPT_VALUES,
            "awslogs-group",  # Invalid: missing colon
            "awslogs-group:aws/batch/jobs",  # Valid on retry
        ]

        result = configure_aws_batch(existing_env)

        # Assert: Error message should be displayed for invalid format
        error_calls = [
            call
            for call in mock_echo.call_args_list
            if "Invalid log option format" in str(call)
        ]
        assert len(error_calls) > 0, "Expected error message for invalid format"

        # Assert: Valid input on retry should be accepted
        assert result["METAFLOW_BATCH_LOG_OPTIONS"] == ["awslogs-group:aws/batch/jobs"]


def test_rejects_non_comma_separated_values():
    """Test that values without colon after comma-split are rejected."""
    # Setup: Provide comma-separated list where one value has no colon
    # This tests that users must use commas to separate multiple options
    existing_env = {}

    with patch("metaflow.cmd.configure_cmd.click.prompt") as mock_prompt, patch(
        "metaflow.cmd.configure_cmd.click.confirm"
    ) as mock_confirm, patch("metaflow.cmd.configure_cmd.echo") as mock_echo:
        mock_confirm.return_value = False

        mock_prompt.side_effect = [
            *MOCK_PROMPT_VALUES,
            "awslogs-group:test,awslogs-prefix",  # Invalid: second value has no colon
            "awslogs-group:test",  # Valid on retry
        ]

        result = configure_aws_batch(existing_env)

        # Assert: Error should be raised for value without colon
        error_calls = [
            call
            for call in mock_echo.call_args_list
            if "Invalid log option format" in str(call) or "Invalid format" in str(call)
        ]
        assert len(error_calls) > 0, "Expected error message for value without colon"

        # Assert: Valid input on retry should be accepted
        assert result["METAFLOW_BATCH_LOG_OPTIONS"] == ["awslogs-group:test"]


def test_resets_log_options():
    """Test that log_options can be reset by passing `none`."""
    # Setup: Provide `none` to reset log options
    existing_env = {"METAFLOW_BATCH_LOG_OPTIONS": ["awslogs-group:old"]}

    with patch("metaflow.cmd.configure_cmd.click.prompt") as mock_prompt, patch(
        "metaflow.cmd.configure_cmd.click.confirm"
    ) as mock_confirm, patch("metaflow.cmd.configure_cmd.echo") as mock_echo:
        mock_confirm.return_value = False

        mock_prompt.side_effect = [
            *MOCK_PROMPT_VALUES,
            "none",  # Reset log options to none
        ]

        result = configure_aws_batch(existing_env)

        # Assert: Log options should be reset to None
        assert result["METAFLOW_BATCH_LOG_OPTIONS"] == None
