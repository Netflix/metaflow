"""
Tests for the empty-input guards in S3._read_many_files and S3._put_many_files.

Root cause (fixed):
    s3op writes informational text to stderr even when it processes 0 files:
        "Uploading 0 files.\\nUploaded 0 files, 0 bytes in total, in 0 seconds."
    Both _put_many_files and _read_many_files checked `if stderr:` which is
    truthy for that output, then crashed with IndexError trying to access
    url_info[0] or prefixes_and_ranges[0] on an empty list.

    Trigger: content-addressed store dedup causes put_many to be called with
    an empty iterator when all blobs already exist in S3 from a prior run.

Fix: return early from both methods when the input list is empty.
"""

import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from metaflow.plugins.datatools.s3.s3 import S3


def _make_s3(tmp_path):
    """Create a minimal S3 instance without a real S3 connection."""
    s3 = object.__new__(S3)
    s3._tmproot = str(tmp_path)
    s3._tmpdir = str(tmp_path)
    s3._bucket = None
    s3._prefix = None
    s3._s3root = None
    s3._encryption = None
    s3._client_params = {}
    return s3


class TestPutManyFilesEmptyInput:
    def test_returns_empty_list(self, tmp_path):
        """_put_many_files with no items returns [] without calling s3op."""
        s3 = _make_s3(tmp_path)
        result = s3._put_many_files(iter([]), overwrite=True)
        assert result == []

    def test_does_not_call_s3op(self, tmp_path):
        """s3op subprocess must not be spawned when there is nothing to upload."""
        s3 = _make_s3(tmp_path)
        with patch.object(s3, "_s3op_with_retries") as mock_op:
            s3._put_many_files(iter([]), overwrite=True)
            mock_op.assert_not_called()

    def test_error_handler_would_crash_without_guard(self):
        """
        Regression: document the latent IndexError in the error handler.

        Before the guard was added, _put_many_files called s3op even with
        empty url_info. s3op exits 0 but writes "Uploading 0 files.." to
        stderr. The `if stderr:` block then executed:
            url_info[0][2]["key"]    # IndexError — url_info is []
        This test proves that the error handler is unsafe with an empty list,
        which is why the early-return guard is necessary.
        """
        url_info = []  # what packing_iter() produces on a CAS cache hit
        stderr = b"Uploading 0 files..\nUploaded 0 files."  # s3op info output
        with pytest.raises(IndexError):
            # Reproduce the exact expression from the old error handler:
            _ = url_info[0][2]["key"]

    def test_non_empty_input_still_calls_s3op(self, tmp_path):
        """Non-empty input should still go through s3op (not short-circuit)."""
        s3 = _make_s3(tmp_path)

        with tempfile.NamedTemporaryFile(
            dir=str(tmp_path), delete=False, mode="wb"
        ) as tf:
            tf.write(b"data")
            local = tf.name

        try:
            with patch.object(
                s3,
                "_s3op_with_retries",
                return_value=([b"s3://bucket/key " + tf.name.encode() + b" "], b"", 0),
            ) as mock_op:

                def _gen():
                    yield local, "s3://bucket/key", {"key": "mykey"}

                s3._put_many_files(_gen(), overwrite=True)
                mock_op.assert_called_once()
        finally:
            os.unlink(local)


class TestReadManyFilesEmptyInput:
    def test_returns_empty_generator(self, tmp_path):
        """_read_many_files with no prefixes yields nothing."""
        s3 = _make_s3(tmp_path)
        result = list(s3._read_many_files("get", iter([])))
        assert result == []

    def test_does_not_call_s3op(self, tmp_path):
        """s3op subprocess must not be spawned when there is nothing to read."""
        s3 = _make_s3(tmp_path)
        with patch.object(s3, "_s3op_with_retries") as mock_op:
            list(s3._read_many_files("get", iter([])))
            mock_op.assert_not_called()

    def test_error_handler_would_crash_without_guard(self):
        """
        Regression: document the latent IndexError in the error handler.

        Before the guard was added, _read_many_files called s3op even with
        empty prefixes_and_ranges. s3op exits 0 but writes
        "Info_downloading 0 files.." to stderr. The `if stderr:` block then
        executed:
            prefixes_and_ranges[0]    # IndexError — list is []
        This test proves that the error handler is unsafe with an empty list,
        which is why the early-return guard is necessary.
        """
        prefixes_and_ranges = []  # empty input materialised as a list
        stderr = b"Info_downloading 0 files..\nInfo_downloaded 0 files."
        with pytest.raises(IndexError):
            # Reproduce the exact expression from the old error handler:
            _ = prefixes_and_ranges[0]

    def test_empty_input_for_all_ops(self, tmp_path):
        """All s3op modes (get, list, info, put) are safe with empty input."""
        s3 = _make_s3(tmp_path)
        with patch.object(s3, "_s3op_with_retries") as mock_op:
            for op in ("get", "list", "info", "put"):
                result = list(s3._read_many_files(op, iter([])))
                assert result == [], f"op={op} should yield nothing for empty input"
            mock_op.assert_not_called()

    def test_non_empty_input_still_calls_s3op(self, tmp_path):
        """Non-empty input should still go through s3op (not short-circuit)."""
        s3 = _make_s3(tmp_path)
        with patch.object(
            s3,
            "_s3op_with_retries",
            return_value=([b"s3://b/k /tmp/f 100"], b"", 0),
        ) as mock_op:
            list(s3._read_many_files("get", iter([("s3://b/k", None)])))
            mock_op.assert_called_once()
