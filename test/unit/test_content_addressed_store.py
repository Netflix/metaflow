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
        assert set(expected_paths).issubset(
            set(paths)
        ), "expected paths %s not all in %s" % (expected_paths, paths)
        return _LoadedBytesContext(self._entries)


def _make_store(entries):
    return ContentAddressedStore("prefix", _FakeStorageImpl(entries))


def _write_blob_file(tmp_path, name="blob.bin", data=b"not-a-valid-gzip-stream"):
    blob_file = tmp_path / name
    blob_file.write_bytes(data)
    return str(blob_file)


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
    ids=["missing_version", "unknown_version", "unpack_failure"],
)
def test_load_blobs_error_message_uses_current_path_key(
    tmp_path, monkeypatch, meta, unpack_error, expected_substrings
):
    stale_key = "aaaaaaaaaa"
    current_key = "bbbbbbbbbb"
    stale_path = "prefix/aa/%s" % stale_key
    current_path = "prefix/bb/%s" % current_key

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
    assert current_path in message
    assert stale_path not in message
    for expected in expected_substrings:
        assert expected in message
