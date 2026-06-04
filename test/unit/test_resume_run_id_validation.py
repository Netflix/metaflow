import pytest

from metaflow.cli_components.run_cmds import _validate_resume_run_id
from metaflow.exception import CommandException, MetaflowNotFound


def test_existing_run_id_raises_helpful_error(mocker):
    """Test that attempting to resume with an existing run ID raises an error."""
    mocker.patch("metaflow.client.core.Run", return_value=mocker.MagicMock())

    with pytest.raises(CommandException) as exc_info:
        _validate_resume_run_id("TestFlow", "ext-scheduler-run")

    msg = str(exc_info.value)
    assert "Run ID ext-scheduler-run already exists" in msg
    assert "resume --origin-run-id ext-scheduler-run" in msg
    assert "resume --run-id ext-scheduler-run" in msg


def test_new_non_integer_run_id_allowed(mocker):
    """Test that a new non-integer run ID is allowed."""
    mocker.patch(
        "metaflow.client.core.Run",
        side_effect=MetaflowNotFound("missing"),
    )

    _validate_resume_run_id("TestFlow", "scheduler-run-abc")


def test_new_integer_run_id_rejected(mocker):
    """Test that a new integer run ID is rejected."""
    mocker.patch(
        "metaflow.client.core.Run",
        side_effect=MetaflowNotFound("missing"),
    )

    with pytest.raises(CommandException, match="cannot be an integer"):
        _validate_resume_run_id("TestFlow", "999")


def test_reentrant_resume_skips_run_id_validation(mocker):
    """Test that reentrant resume skips run ID validation."""
    run_mock = mocker.patch("metaflow.client.core.Run")

    _validate_resume_run_id("TestFlow", "123", reentrant=True)

    run_mock.assert_not_called()


def test_no_run_id_skips_validation(mocker):
    """Test that validation is skipped when no run ID is provided."""
    run_mock = mocker.patch("metaflow.client.core.Run")

    _validate_resume_run_id("TestFlow", None)

    run_mock.assert_not_called()
