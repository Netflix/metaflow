from io import BytesIO

import metaflow.plugins.datastores.s3_storage as s3_storage_module
from metaflow.plugins.datastores.s3_storage import S3Storage


class DummyS3(object):
    last_instance = None

    def __init__(self, **kwargs):
        self.put_many_calls = []
        self.put_calls = []
        DummyS3.last_instance = self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def put_many(self, objs, overwrite):
        self.put_many_calls.append((list(objs), overwrite))

    def put(self, key, obj, overwrite=False, metadata=None):
        self.put_calls.append((key, obj, overwrite, metadata))


def _make_storage():
    storage = object.__new__(S3Storage)
    storage.datastore_root = "s3://unit-test-root"
    storage.s3_client = object()
    return storage


def test_save_bytes_put_many_preserves_metadata_slot(monkeypatch):
    monkeypatch.setattr(s3_storage_module, "S3", DummyS3)
    storage = _make_storage()

    storage.save_bytes(
        iter(
            [
                ("a", (BytesIO(b"abc"), {"k": "v"})),
                ("b", BytesIO(b"def")),
            ]
        ),
        overwrite=True,
        len_hint=11,
    )

    put_objs, overwrite = DummyS3.last_instance.put_many_calls[0]
    assert overwrite is True
    assert put_objs[0].encryption is None
    assert put_objs[0].metadata == {"k": "v"}
    assert put_objs[1].encryption is None
    assert put_objs[1].metadata is None


def test_save_bytes_sequential_preserves_metadata(monkeypatch):
    monkeypatch.setattr(s3_storage_module, "S3", DummyS3)
    storage = _make_storage()

    storage.save_bytes(
        iter(
            [
                ("a", (BytesIO(b"abc"), {"k": "v"})),
                ("b", BytesIO(b"def")),
            ]
        ),
        overwrite=False,
        len_hint=2,
    )

    put_calls = DummyS3.last_instance.put_calls
    assert len(put_calls) == 2
    assert put_calls[0][0] == "a"
    assert put_calls[0][3] == {"k": "v"}
    assert put_calls[1][0] == "b"
    assert put_calls[1][3] is None
