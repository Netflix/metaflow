"""
Regression tests for the wall-clock-vs-monotonic fix in CardProcessManager.

These tests verify that the async card-process timeout uses time.monotonic()
(immune to wall-clock jumps) rather than time.time() (which can step backward
under NTP correction, container clock skew, or settimeofday).

Both call sites in CardCreator._run_command (sync + async branches) and
CardProcessManager._register_card_process must use the same monotonic source
so the elapsed-time arithmetic is meaningful.
"""

from unittest.mock import patch, MagicMock

import pytest

from metaflow.plugins.cards.card_creator import CardCreator, CardProcessManager


@pytest.fixture(autouse=True)
def _clean_registry():
    CardProcessManager.async_card_processes.clear()
    yield
    CardProcessManager.async_card_processes.clear()


def _fake_running_proc():
    proc = MagicMock()
    proc.poll.return_value = None  # still running
    proc.kill = MagicMock()
    return proc


def test_register_uses_monotonic_not_wall_clock():
    """
    _register_card_process must read time.monotonic, not time.time.

    Patching time.time to an absurd value would have leaked through to
    'started' under the old implementation.
    """
    proc = _fake_running_proc()
    with patch("metaflow.plugins.cards.card_creator.time.time", return_value=1.0):
        with patch(
            "metaflow.plugins.cards.card_creator.time.monotonic", return_value=42.0
        ):
            CardProcessManager._register_card_process("uuid-a", proc)

    _, started = CardProcessManager._get_card_process("uuid-a")
    assert started == 42.0, "registry must store the monotonic timestamp"


def test_timeout_check_immune_to_wall_clock_backward_jump():
    """
    Regression: under the old time.time() implementation, a backward
    wall-clock jump (NTP correction, container clock skew, manual
    settimeofday) could make the elapsed delta negative, causing the
    subprocess to leak forever.

    With time.monotonic(), the elapsed delta is unaffected by wall-clock
    jumps, so a process that has run past the timeout still gets reaped.
    """
    proc = _fake_running_proc()

    # Register with monotonic at t=100
    with patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=100.0
    ):
        CardProcessManager._register_card_process("uuid-b", proc)

    creator = CardCreator(top_level_options={}, should_save_metadata_lambda=lambda _: (False, {}))

    # Simulate: wall clock jumped 1 day backward, but monotonic advanced 600s
    # (10 minutes), comfortably past the 60s timeout.
    with patch(
        "metaflow.plugins.cards.card_creator.time.time",
        return_value=-86400.0,  # absurd wall-clock backward jump
    ):
        with patch(
            "metaflow.plugins.cards.card_creator.time.monotonic", return_value=700.0
        ):
            creator._wait_for_async_processes_to_finish("uuid-b", async_timeout=60)

    # Process must have been killed and removed despite the backward wall-clock jump.
    proc.kill.assert_called_once()
    assert "uuid-b" not in CardProcessManager.async_card_processes


def test_timeout_check_not_triggered_when_within_window():
    """
    Sanity: when monotonic elapsed time is still within the timeout window,
    the running process is left alone.
    """
    proc = _fake_running_proc()

    with patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=100.0
    ):
        CardProcessManager._register_card_process("uuid-c", proc)

    creator = CardCreator(top_level_options={}, should_save_metadata_lambda=lambda _: (False, {}))

    # Only 5s of monotonic elapsed, timeout is 60s. Use side_effect to break
    # the while-loop after the first iteration so the test doesn't hang.
    proc.poll.side_effect = [None, 0]  # running, then finished

    with patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=105.0
    ):
        creator._wait_for_async_processes_to_finish("uuid-c", async_timeout=60)

    proc.kill.assert_not_called()
    # Process remains registered (timeout did not fire).
    assert "uuid-c" in CardProcessManager.async_card_processes
