from unittest.mock import MagicMock, patch

import pytest

from metaflow.cli_components.run_cmds import _validate_resume_run_id
from metaflow.exception import CommandException, MetaflowNotFound


def test_existing_run_id_raises_helpful_error():
    with patch("metaflow.client.core.Run", return_value=MagicMock()):
        with pytest.raises(CommandException) as exc_info:
            _validate_resume_run_id("TestFlow", "123")

    msg = str(exc_info.value)
    assert "Run ID 123 already exists" in msg
    assert "resume --origin-run-id 123" in msg
    assert "resume --run-id 123" in msg


def test_new_non_integer_run_id_allowed():
    with patch(
        "metaflow.client.core.Run",
        side_effect=MetaflowNotFound("missing"),
    ):
        _validate_resume_run_id("TestFlow", "scheduler-run-abc")


def test_new_integer_run_id_rejected():
    with patch(
        "metaflow.client.core.Run",
        side_effect=MetaflowNotFound("missing"),
    ):
        with pytest.raises(CommandException, match="cannot be an integer"):
            _validate_resume_run_id("TestFlow", "999")


def test_reentrant_resume_skips_run_id_validation():
    with patch("metaflow.client.core.Run") as run_mock:
        _validate_resume_run_id("TestFlow", "123", reentrant=True)

    run_mock.assert_not_called()


def test_no_run_id_skips_validation():
    with patch("metaflow.client.core.Run") as run_mock:
        _validate_resume_run_id("TestFlow", None)

    run_mock.assert_not_called()
