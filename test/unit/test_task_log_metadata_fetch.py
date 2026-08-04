"""Regression tests for redundant metadata fetches on task log accessors (#3034)."""

import pytest

from metaflow.client.core import Task

PATH_COMPONENTS = ("TestFlow", "123", "start", "1")

LOG_METADATA = {
    "ds-type": "local",
    "ds-root": "/tmp/logs",
    "attempt": "2",
}

SIZE_METADATA = {
    "ds-type": "local",
    "ds-root": "/tmp/logs",
    "attempt": "3",
}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def minimal_task(mocker):
    task = Task.__new__(Task)
    task._attempt = None
    task._path_components = list(PATH_COMPONENTS)
    task._metaflow = mocker.Mock()
    return task


@pytest.fixture
def metadata_dict_mock(mocker):
    return mocker.patch.object(Task, "metadata_dict", new_callable=mocker.PropertyMock)


@pytest.fixture
def filecache_cls(mocker):
    mocker.patch("metaflow.client.core.filecache", None)
    return mocker.patch("metaflow.client.core.FileCache")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stream", ["stdout", "stderr"], ids=["stdout_stream", "stderr_stream"]
)
def test_loglines_uses_supplied_metadata_without_refetching(
    minimal_task, metadata_dict_mock, filecache_cls, mocker, stream
):
    filecache_cls.return_value.get_logs_stream.return_value = []
    merge_logs = mocker.patch("metaflow.mflog.mflog.merge_logs", return_value=[])

    assert list(minimal_task.loglines(stream, meta_dict=LOG_METADATA.copy())) == []

    metadata_dict_mock.assert_not_called()
    filecache_cls.return_value.get_logs_stream.assert_called_once_with(
        "local", "/tmp/logs", stream, 2, *PATH_COMPONENTS
    )
    merge_logs.assert_called_once_with([])


@pytest.mark.parametrize(
    "stream", ["stdout", "stderr"], ids=["stdout_stream", "stderr_stream"]
)
def test_log_size_uses_supplied_metadata_without_refetching(
    minimal_task, metadata_dict_mock, filecache_cls, stream
):
    filecache_cls.return_value.get_log_size.return_value = 42

    assert minimal_task._log_size(stream, SIZE_METADATA.copy()) == 42

    metadata_dict_mock.assert_not_called()
    filecache_cls.return_value.get_log_size.assert_called_once_with(
        "local", "/tmp/logs", stream, 3, *PATH_COMPONENTS
    )


@pytest.mark.parametrize(
    "explicit_attempt, meta_dict, expected",
    [
        (5, {"attempt": "0"}, 5),
        (None, {"attempt": "2"}, 2),
        (None, {}, 0),
    ],
    ids=[
        "explicit_overrides_metadata",
        "metadata_overrides_default",
        "defaults_to_zero",
    ],
)
def test_resolve_log_attempt_prefers_explicit_attempt_then_metadata(
    minimal_task, explicit_attempt, meta_dict, expected
):
    minimal_task._attempt = explicit_attempt

    assert minimal_task._resolve_log_attempt(meta_dict) == expected


def test_resolve_log_attempt_delegates_when_metadata_not_provided(minimal_task, mocker):
    current_attempt = mocker.patch.object(
        Task, "current_attempt", new_callable=mocker.PropertyMock
    )
    current_attempt.return_value = 7

    assert minimal_task._resolve_log_attempt(None) == 7
    current_attempt.assert_called_once_with()
