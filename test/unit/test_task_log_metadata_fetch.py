"""Regression tests for redundant metadata fetches on task log accessors (#3034)."""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from metaflow.client.core import Task


def _minimal_task():
    task = Task.__new__(Task)
    task._attempt = None
    task._path_components = ["TestFlow", "123", "start", "1"]
    task._metaflow = MagicMock()
    return task


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_loglines_does_not_refetch_metadata_when_meta_dict_provided(stream):
    meta_dict = {
        "ds-type": "local",
        "ds-root": "/tmp/logs",
        "attempt": "2",
    }
    task = _minimal_task()

    with patch.object(
        Task, "metadata_dict", new_callable=PropertyMock
    ) as metadata_dict_mock:
        with patch("metaflow.client.core.filecache", None):
            with patch("metaflow.client.core.FileCache") as filecache_cls:
                filecache_cls.return_value.get_logs_stream.return_value = []
                with patch(
                    "metaflow.mflog.mflog.merge_logs", return_value=[]
                ):
                    list(task.loglines(stream, meta_dict=meta_dict))

        metadata_dict_mock.assert_not_called()


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_log_size_does_not_refetch_metadata_when_meta_dict_provided(stream):
    meta_dict = {
        "ds-type": "local",
        "ds-root": "/tmp/logs",
        "attempt": "3",
    }
    task = _minimal_task()

    with patch.object(
        Task, "metadata_dict", new_callable=PropertyMock
    ) as metadata_dict_mock:
        with patch("metaflow.client.core.filecache", None):
            with patch("metaflow.client.core.FileCache") as filecache_cls:
                filecache_cls.return_value.get_log_size.return_value = 42
                size = task._log_size(stream, meta_dict)

        metadata_dict_mock.assert_not_called()
        assert size == 42


def test_resolve_log_attempt_prefers_explicit_attempt():
    task = _minimal_task()
    task._attempt = 5
    assert task._resolve_log_attempt({"attempt": "0"}) == 5


def test_resolve_log_attempt_reads_from_meta_dict():
    task = _minimal_task()
    assert task._resolve_log_attempt({"attempt": "2"}) == 2


def test_resolve_log_attempt_defaults_missing_attempt_to_zero():
    task = _minimal_task()
    assert task._resolve_log_attempt({}) == 0
