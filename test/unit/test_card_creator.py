"""Regression tests for async card process timeout handling."""

import subprocess

import pytest

from metaflow.plugins.cards.card_creator import CardCreator, CardProcessManager

CARD_UUID = "card-uuid"
ASYNC_TIMEOUT = 60


@pytest.fixture(autouse=True)
def clean_card_process_registry():
    CardProcessManager.async_card_processes.clear()
    yield
    CardProcessManager.async_card_processes.clear()


@pytest.fixture
def running_process(mocker):
    process = mocker.Mock()
    process.poll.return_value = None
    return process


@pytest.fixture
def card_creator():
    return CardCreator(
        top_level_options=[], should_save_metadata_lambda=lambda _: (False, {})
    )


def test_register_card_process_uses_monotonic_timestamp(mocker, running_process):
    mocker.patch("metaflow.plugins.cards.card_creator.time.time", return_value=1.0)
    mocker.patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=42.0
    )

    CardProcessManager._register_card_process(CARD_UUID, running_process)

    _, started = CardProcessManager._get_card_process(CARD_UUID)
    assert started == 42.0


def test_wait_for_async_process_ignores_backward_wall_clock_jump(
    mocker, card_creator, running_process
):
    mocker.patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=100.0
    )
    CardProcessManager._register_card_process(CARD_UUID, running_process)
    running_process.poll.side_effect = [None, 0]
    mocker.patch("metaflow.plugins.cards.card_creator.time.time", return_value=-86400.0)
    mocker.patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=700.0
    )

    card_creator._wait_for_async_processes_to_finish(
        CARD_UUID, async_timeout=ASYNC_TIMEOUT
    )

    running_process.kill.assert_called_once_with()
    assert CARD_UUID not in CardProcessManager.async_card_processes


def test_wait_for_async_process_leaves_process_within_timeout(
    mocker, card_creator, running_process
):
    mocker.patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=100.0
    )
    CardProcessManager._register_card_process(CARD_UUID, running_process)
    running_process.poll.side_effect = [None, 0]
    mocker.patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=105.0
    )

    card_creator._wait_for_async_processes_to_finish(
        CARD_UUID, async_timeout=ASYNC_TIMEOUT
    )

    running_process.kill.assert_not_called()
    assert CARD_UUID in CardProcessManager.async_card_processes


def test_async_run_replaces_timed_out_process(mocker, card_creator, running_process):
    replacement_process = mocker.Mock()
    popen = mocker.patch(
        "metaflow.plugins.cards.card_creator.subprocess.Popen",
        return_value=replacement_process,
    )
    mocker.patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=100.0
    )
    CardProcessManager._register_card_process(CARD_UUID, running_process)
    mocker.patch("metaflow.plugins.cards.card_creator.time.time", return_value=-86400.0)
    mocker.patch(
        "metaflow.plugins.cards.card_creator.time.monotonic", return_value=700.0
    )

    output, failed = card_creator._run_command(
        ["python", "card.py"], CARD_UUID, {"KEY": "value"}, wait=False
    )

    assert output == b""
    assert failed is False
    running_process.kill.assert_called_once_with()
    popen.assert_called_once_with(
        ["python", "card.py"],
        env={"KEY": "value"},
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )
    process, _ = CardProcessManager._get_card_process(CARD_UUID)
    assert process is replacement_process
