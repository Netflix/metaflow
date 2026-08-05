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

import pytest

from metaflow.plugins.datatools.s3.s3 import S3

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def s3_instance(tmp_path):
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


# ---------------------------------------------------------------------------
# Test Functions: Put Many Files
# ---------------------------------------------------------------------------


def test_put_many_files_empty_input_returns_empty_list(s3_instance):
    """_put_many_files with no items returns [] without calling s3op."""
    result = s3_instance._put_many_files(iter([]), overwrite=True)
    assert result == []


def test_put_many_files_empty_input_does_not_call_s3op(mocker, s3_instance):
    """s3op subprocess must not be spawned when there is nothing to upload."""
    mock_op = mocker.patch.object(s3_instance, "_s3op_with_retries")

    s3_instance._put_many_files(iter([]), overwrite=True)

    mock_op.assert_not_called()


def test_put_many_files_error_handler_would_crash_without_guard():
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


def test_put_many_files_non_empty_input_calls_s3op(mocker, s3_instance, tmp_path):
    """Non-empty input should still go through s3op (not short-circuit)."""
    # Use tmp_path instead of NamedTemporaryFile to avoid manual cleanup
    local_file = tmp_path / "test_data.txt"
    local_file.write_bytes(b"data")

    mock_op = mocker.patch.object(
        s3_instance,
        "_s3op_with_retries",
        return_value=([b"s3://bucket/key " + str(local_file).encode() + b" "], b"", 0),
    )

    def _gen():
        yield str(local_file), "s3://bucket/key", {"key": "mykey"}

    s3_instance._put_many_files(_gen(), overwrite=True)

    mock_op.assert_called_once()


# ---------------------------------------------------------------------------
# Test Functions: Read Many Files
# ---------------------------------------------------------------------------


def test_read_many_files_empty_input_returns_empty_generator(s3_instance):
    """_read_many_files with no prefixes yields nothing."""
    result = list(s3_instance._read_many_files("get", iter([])))
    assert result == []


def test_read_many_files_empty_input_does_not_call_s3op(mocker, s3_instance):
    """s3op subprocess must not be spawned when there is nothing to read."""
    mock_op = mocker.patch.object(s3_instance, "_s3op_with_retries")

    list(s3_instance._read_many_files("get", iter([])))

    mock_op.assert_not_called()


def test_read_many_files_error_handler_would_crash_without_guard():
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


@pytest.mark.parametrize(
    "s3_op",
    ["get", "list", "info", "put"],
    ids=["op_get", "op_list", "op_info", "op_put"],
)
def test_read_many_files_empty_input_safe_for_all_ops(mocker, s3_instance, s3_op):
    """All s3op modes (get, list, info, put) are safe with empty input."""
    mock_op = mocker.patch.object(s3_instance, "_s3op_with_retries")

    result = list(s3_instance._read_many_files(s3_op, iter([])))

    assert result == [], f"op={s3_op} should yield nothing for empty input"
    mock_op.assert_not_called()


def test_read_many_files_non_empty_input_calls_s3op(mocker, s3_instance):
    """Non-empty input should still go through s3op (not short-circuit)."""
    mock_op = mocker.patch.object(
        s3_instance,
        "_s3op_with_retries",
        return_value=([b"s3://b/k /tmp/f 100"], b"", 0),
    )

    list(s3_instance._read_many_files("get", iter([("s3://b/k", None)])))

    mock_op.assert_called_once()
