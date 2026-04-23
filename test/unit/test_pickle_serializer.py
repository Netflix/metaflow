import pickle

import pytest

from metaflow.datastore.artifacts.serializer import (
    SerializationMetadata,
    SerializerStore,
)
from metaflow.plugins.datastores.serializers.pickle_serializer import PickleSerializer


# ---------------------------------------------------------------------------
# Registration and identity
# ---------------------------------------------------------------------------


def test_type_is_pickle():
    assert PickleSerializer.TYPE == "pickle"


def test_priority_is_fallback():
    assert PickleSerializer.PRIORITY == 9999


def test_registered_in_store():
    assert "pickle" in SerializerStore._all_serializers
    assert SerializerStore._all_serializers["pickle"] is PickleSerializer


def test_last_in_ordering():
    """PickleSerializer should be last (highest PRIORITY) among registered serializers."""
    ordered = SerializerStore.get_ordered_serializers()
    assert ordered[-1] is PickleSerializer


# ---------------------------------------------------------------------------
# can_serialize
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "obj",
    [
        42,
        "hello",
        3.14,
        None,
        True,
        [1, 2, 3],
        {"key": "value"},
        (1, "a"),
        set([1, 2]),
        b"bytes",
        object(),
    ],
    ids=[
        "int",
        "str",
        "float",
        "None",
        "bool",
        "list",
        "dict",
        "tuple",
        "set",
        "bytes",
        "object",
    ],
)
def test_can_serialize_any_object(obj):
    assert PickleSerializer.can_serialize(obj) is True


# ---------------------------------------------------------------------------
# can_deserialize
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "encoding",
    ["pickle-v2", "pickle-v4", "gzip+pickle-v2", "gzip+pickle-v4"],
)
def test_can_deserialize_valid_encodings(encoding):
    meta = SerializationMetadata("object", 100, encoding, {})
    assert PickleSerializer.can_deserialize(meta) is True


@pytest.mark.parametrize(
    "encoding",
    ["json", "iotype:text", "msgpack", "unknown", ""],
)
def test_cannot_deserialize_unknown_encodings(encoding):
    meta = SerializationMetadata("object", 100, encoding, {})
    assert PickleSerializer.can_deserialize(meta) is False


# ---------------------------------------------------------------------------
# serialize
# ---------------------------------------------------------------------------


def test_serialize_returns_single_blob():
    blobs, meta = PickleSerializer.serialize({"key": "value"})
    assert len(blobs) == 1
    assert blobs[0].needs_save is True
    assert blobs[0].is_reference is False


def test_serialize_metadata_encoding():
    _, meta = PickleSerializer.serialize(42)
    assert meta.encoding == "pickle-v4"


def test_serialize_metadata_type():
    _, meta = PickleSerializer.serialize([1, 2, 3])
    assert "list" in meta.type


def test_serialize_metadata_size():
    obj = {"a": 1, "b": 2}
    blobs, meta = PickleSerializer.serialize(obj)
    assert meta.size == len(blobs[0].value)
    assert meta.size > 0


def test_serialize_metadata_serializer_info_empty():
    _, meta = PickleSerializer.serialize("hello")
    assert meta.serializer_info == {}


def test_serialize_compress_method():
    blobs, _ = PickleSerializer.serialize(42)
    assert blobs[0].compress_method == "gzip"


# ---------------------------------------------------------------------------
# Round-trip: serialize -> deserialize
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "obj",
    [
        42,
        "hello world",
        3.14,
        None,
        True,
        False,
        [1, "two", 3.0],
        {"nested": {"key": [1, 2, 3]}},
        (1, 2, 3),
        set([1, 2, 3]),
        b"raw bytes",
    ],
    ids=[
        "int",
        "str",
        "float",
        "None",
        "True",
        "False",
        "list",
        "nested_dict",
        "tuple",
        "set",
        "bytes",
    ],
)
def test_round_trip(obj):
    blobs, meta = PickleSerializer.serialize(obj)
    raw_blobs = [b.value for b in blobs]
    result = PickleSerializer.deserialize(raw_blobs, meta, context=None)
    assert result == obj


class _CustomObj:
    def __init__(self, x):
        self.x = x

    def __eq__(self, other):
        return isinstance(other, _CustomObj) and self.x == other.x


def test_round_trip_custom_class():
    obj = _CustomObj(42)
    blobs, meta = PickleSerializer.serialize(obj)
    raw_blobs = [b.value for b in blobs]
    result = PickleSerializer.deserialize(raw_blobs, meta, context=None)
    assert result == obj
    assert result.x == 42
