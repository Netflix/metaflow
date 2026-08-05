"""Tests for S3 datastore byte saving and metadata mapping."""

from io import BytesIO

import pytest

import metaflow.plugins.datastores.s3_storage as s3_storage_module
from metaflow.plugins.datastores.s3_storage import S3Storage


# --- Fixtures ---


@pytest.fixture
def s3_storage():
    """Fixture providing a minimal, uninitialized S3Storage instance."""
    storage = object.__new__(S3Storage)
    storage.datastore_root = "s3://unit-test-root"
    storage.s3_client = object()
    return storage


@pytest.fixture
def test_items():
    """Fresh BytesIO objects per test so cursor state does not bleed."""
    return [
        ("a", (BytesIO(b"abc"), {"k": "v"})),
        ("b", BytesIO(b"def")),
    ]


@pytest.fixture
def patched_s3(mocker):
    """Mock the S3 client used by S3Storage.save_bytes."""
    s3 = mocker.MagicMock()
    s3_cm = mocker.MagicMock()
    s3_cm.__enter__.return_value = s3
    s3_cm.__exit__.return_value = False
    mocker.patch.object(s3_storage_module, "S3", return_value=s3_cm)
    return s3


# --- Tests ---


def test_save_bytes_put_many_preserves_metadata_slot(
    s3_storage, patched_s3, test_items
):
    """
    When len_hint > 10, save_bytes optimizes by delegating to put_many.
    This test ensures the metadata payload survives the batch translation.
    """
    s3_storage.save_bytes(iter(test_items), overwrite=True, len_hint=11)

    put_objs, overwrite = patched_s3.put_many.call_args[0]
    put_objs = list(put_objs)

    assert overwrite is True
    assert put_objs[0].encryption is None
    assert put_objs[0].metadata == {"k": "v"}
    assert put_objs[1].encryption is None
    assert put_objs[1].metadata is None


def test_save_bytes_sequential_preserves_metadata(s3_storage, patched_s3, test_items):
    """
    When len_hint <= 10, save_bytes falls back to sequential put() calls.
    This test ensures the metadata kwargs are passed correctly per item.
    """
    s3_storage.save_bytes(iter(test_items), overwrite=False, len_hint=2)

    put_calls = patched_s3.put.call_args_list
    assert len(put_calls) == 2

    # Check first item ("a")
    assert put_calls[0][0][0] == "a"
    assert put_calls[0][1]["metadata"] == {"k": "v"}

    # Check second item ("b")
    assert put_calls[1][0][0] == "b"
    assert put_calls[1][1]["metadata"] is None
