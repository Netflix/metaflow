import pytest

from metaflow.datastore.content_addressed_store import ContentAddressedStore
from metaflow.datastore.exceptions import DataException


class _LoadedBytesContext(object):
    def __init__(self, entries):
        self._entries = entries

    def __enter__(self):
        return iter(self._entries)

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeStorageImpl(object):
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
        return "fake://" + path

    def load_bytes(self, paths):
        expected_paths = [entry[0] for entry in self._entries]
        assert paths == expected_paths, "unexpected paths: %s" % paths
        return _LoadedBytesContext(self._entries)


def _make_store(entries):
    return ContentAddressedStore("prefix", _FakeStorageImpl(entries))


def _write_blob_file(tmp_path, name="blob.bin", data=b"not-a-valid-gzip-stream"):
    blob_file = tmp_path / name
    blob_file.write_bytes(data)
    return str(blob_file)


def test_load_blobs_uses_current_path_key_when_version_missing(tmp_path):
    stale_key = "aaaaaaaaaa"
    current_key = "bbbbbbbbbb"

    stale_path = "prefix/aa/%s" % stale_key
    current_path = "prefix/bb/%s" % current_key

    file_path = _write_blob_file(tmp_path)

    store = _make_store([(current_path, file_path, {})])

    # Keep stale_key last on purpose:
    # the buggy code reuses the outer-loop `path`, which retains the path for
    # the last key processed. `load_bytes()` returns only `current_path`, so
    # this ordering makes stale `path` differ from current `path_key`.
    with pytest.raises(DataException) as exc:
        list(store.load_blobs([current_key, stale_key]))

    message = str(exc.value)
    assert current_path in message
    assert stale_path not in message
    assert "Could not extract encoding version" in message


def test_load_blobs_uses_current_path_key_for_unknown_encoding_version(tmp_path):
    stale_key = "aaaaaaaaaa"
    current_key = "bbbbbbbbbb"

    stale_path = "prefix/aa/%s" % stale_key
    current_path = "prefix/bb/%s" % current_key

    file_path = _write_blob_file(tmp_path)

    store = _make_store(
        [(current_path, file_path, {"cas_version": 999, "cas_raw": False})]
    )

    # Keep stale_key last on purpose:
    # the buggy code leaves `path` pointing at the last outer-loop key, while
    # `path_key` comes from the current `load_bytes()` entry.
    with pytest.raises(DataException) as exc:
        list(store.load_blobs([current_key, stale_key]))

    message = str(exc.value)
    assert "Unknown encoding version 999" in message
    assert current_path in message
    assert stale_path not in message


def test_load_blobs_uses_current_path_key_when_unpack_fails(tmp_path, monkeypatch):
    stale_key = "aaaaaaaaaa"
    current_key = "bbbbbbbbbb"

    stale_path = "prefix/aa/%s" % stale_key
    current_path = "prefix/bb/%s" % current_key

    file_path = _write_blob_file(tmp_path)

    store = _make_store(
        [(current_path, file_path, {"cas_version": 1, "cas_raw": False})]
    )

    def _raise_unpack_error(_fileobj):
        raise ValueError("boom")

    monkeypatch.setattr(store, "_unpack_v1", _raise_unpack_error)

    # Keep stale_key last on purpose:
    # this ensures stale outer-loop `path` is `stale_path`, while the failing
    # blob's actual `path_key` is `current_path`.
    with pytest.raises(DataException) as exc:
        list(store.load_blobs([current_key, stale_key]))

    message = str(exc.value)
    assert "Could not unpack artifact" in message
    assert current_path in message
    assert stale_path not in message
    assert "boom" in message
