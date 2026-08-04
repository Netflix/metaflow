"""Regression tests for async card process timeout handling.

These tests ensure that asynchronous card creation processes are reliably managed,
properly timed out, and remain resilient to system clock variations (like NTP syncs).
"""

import subprocess

import pytest

from metaflow.plugins.cards.card_creator import CardCreator, CardProcessManager

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CARD_UUID = "card-uuid"
ASYNC_TIMEOUT = 60

# Module paths for cleaner mocking
MODULE_TIME = "metaflow.plugins.cards.card_creator.time"
MODULE_SUBPROCESS = "metaflow.plugins.cards.card_creator.subprocess"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_card_process_registry():
    """Ensure a pristine process registry before and after every test."""
    CardProcessManager.async_card_processes.clear()
    yield
    CardProcessManager.async_card_processes.clear()


@pytest.fixture
def running_process(mocker):
    """Provide a mock subprocess that appears to be actively running."""
    process = mocker.Mock()
    process.poll.return_value = None
    return process


@pytest.fixture
def card_creator():
    """Provide a minimally configured CardCreator instance."""
    return CardCreator(
        top_level_options=[], should_save_metadata_lambda=lambda _: (False, {})
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_register_card_process_uses_monotonic_timestamp(mocker, running_process):
    """Verify that process registration relies on monotonic time, not wall-clock time."""
    mocker.patch(f"{MODULE_TIME}.time", return_value=1.0)
    mocker.patch(f"{MODULE_TIME}.monotonic", return_value=42.0)

    CardProcessManager._register_card_process(CARD_UUID, running_process)
    _, started = CardProcessManager._get_card_process(CARD_UUID)

    assert started == 42.0


def test_wait_for_async_process_ignores_backward_wall_clock_jump(
    mocker, card_creator, running_process
):
    """Ensure a backward jump in the system clock (e.g., NTP sync) does not bypass the timeout logic."""
    mocker.patch(f"{MODULE_TIME}.monotonic", return_value=100.0)
    CardProcessManager._register_card_process(CARD_UUID, running_process)

    running_process.poll.side_effect = [None, 0]
    mocker.patch(
        f"{MODULE_TIME}.time", return_value=-86400.0
    )  # Extreme backward wall-clock jump
    mocker.patch(
        f"{MODULE_TIME}.monotonic", return_value=700.0
    )  # Monotonic time correctly progresses

    card_creator._wait_for_async_processes_to_finish(
        CARD_UUID, async_timeout=ASYNC_TIMEOUT
    )

    running_process.kill.assert_called_once_with()
    assert CARD_UUID not in CardProcessManager.async_card_processes


def test_wait_for_async_process_leaves_process_within_timeout(
    mocker, card_creator, running_process
):
    """Ensure processes are allowed to continue running if the timeout threshold has not been breached."""
    mocker.patch(f"{MODULE_TIME}.monotonic", return_value=100.0)
    CardProcessManager._register_card_process(CARD_UUID, running_process)

    running_process.poll.side_effect = [None, 0]
    mocker.patch(
        f"{MODULE_TIME}.monotonic", return_value=105.0
    )  # Only 5 seconds elapsed

    card_creator._wait_for_async_processes_to_finish(
        CARD_UUID, async_timeout=ASYNC_TIMEOUT
    )

    running_process.kill.assert_not_called()
    assert CARD_UUID in CardProcessManager.async_card_processes


def test_async_run_replaces_timed_out_process(mocker, card_creator, running_process):
    """Verify that launching a new async command correctly kills and replaces an existing timed-out process."""
    replacement_process = mocker.Mock()
    popen = mocker.patch(
        f"{MODULE_SUBPROCESS}.Popen",
        return_value=replacement_process,
    )

    # Register the initial process
    mocker.patch(f"{MODULE_TIME}.monotonic", return_value=100.0)
    CardProcessManager._register_card_process(CARD_UUID, running_process)

    # Simulate time passing beyond the timeout threshold
    mocker.patch(f"{MODULE_TIME}.time", return_value=-86400.0)
    mocker.patch(f"{MODULE_TIME}.monotonic", return_value=700.0)

    # Attempt to run a new command with the same UUID
    output, failed = card_creator._run_command(
        ["python", "card.py"], CARD_UUID, {"KEY": "value"}, wait=False
    )

    # Assertions
    assert output == b""
    assert failed is False

    running_process.kill.assert_called_once_with()
    popen.assert_called_once_with(
        ["python", "card.py"],
        env={"KEY": "value"},
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )

    # Verify the registry holds the new process
    process, _ = CardProcessManager._get_card_process(CARD_UUID)
    assert process is replacement_process
