import contextlib
from pathlib import Path

import pytest

from metaflow.datastore.content_addressed_store import ContentAddressedStore
from metaflow.datastore.exceptions import DataException

# ---------------------------------------------------------------------------
# Mocks & Helpers
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _loaded_bytes(entries):
    """Context manager to simulate loading bytes iteratively."""
    yield iter(entries)


class _FakeStorageImpl:
    """A minimal fake storage implementation to support CAS loading tests."""

    TYPE = "fake"

    def __init__(self, entries):
        self._entries = entries

    @staticmethod
    def path_join(*parts):
        return "/".join(parts)

    @staticmethod
    def path_split(path):
        return path.split("/")

    @staticmethod
    def full_uri(path):
        return f"fake://{path}"

    def load_bytes(self, paths):
        expected_paths = {entry[0] for entry in self._entries}
        assert expected_paths.issubset(
            paths
        ), f"expected paths {expected_paths} not all in {paths}"
        return _loaded_bytes(self._entries)


def _make_store(entries):
    """Helper to initialize a ContentAddressedStore with fake storage."""
    return ContentAddressedStore("prefix", _FakeStorageImpl(entries))


def _write_blob_file(tmp_path: Path, name="blob.bin", data=b"not-a-valid-gzip-stream"):
    """Helper to write a temporary binary blob file."""
    blob_file = tmp_path / name
    blob_file.write_bytes(data)
    return str(blob_file)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "meta, unpack_error, expected_substrings",
    [
        ({}, None, ["Could not extract encoding version"]),
        (
            {"cas_version": 999, "cas_raw": False},
            None,
            ["Unknown encoding version 999"],
        ),
        (
            {"cas_version": 1, "cas_raw": False},
            "boom",
            ["Could not unpack artifact", "boom"],
        ),
    ],
    ids=["missing-version", "unknown-version", "unpack-failure"],
)
def test_load_blobs_error_message_uses_current_path_key(
    tmp_path, monkeypatch, meta, unpack_error, expected_substrings
):
    """
    Verify that load_blobs error messages accurately reference the *current*
    failing path, preventing regression of a bug where a stale outer-loop
    variable caused misleading exception messages.
    """
    stale_key = "aaaaaaaaaa"
    current_key = "bbbbbbbbbb"
    stale_path = f"prefix/aa/{stale_key}"
    current_path = f"prefix/bb/{current_key}"

    file_path = _write_blob_file(tmp_path)
    store = _make_store([(current_path, file_path, meta)])

    if unpack_error is not None:

        def _raise_unpack_error(_fileobj):
            raise ValueError(unpack_error)

        monkeypatch.setattr(store, "_unpack_v1", _raise_unpack_error)

    # Order keys so the buggy outer-loop `path` would differ from the current
    # `path_key`: load_bytes() returns only current_path, but the buggy code's
    # stale `path` would retain stale_path (the last outer-loop iteration).
    with pytest.raises(DataException) as exc:
        list(store.load_blobs([current_key, stale_key]))

    message = str(exc.value)

    # Assertions
    assert (
        current_path in message
    ), f"Expected active path {current_path} in error message."
    assert (
        stale_path not in message
    ), f"Stale path {stale_path} leaked into the error message."

    for expected in expected_substrings:
        assert expected in message
