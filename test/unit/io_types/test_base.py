"""Contract tests for the IOType ABC."""

import pytest

from metaflow.datastore.artifacts.serializer import STORAGE, WIRE, SerializedBlob
from metaflow.io_types import IOType


class _TextIOType(IOType):
    """Minimal concrete IOType used only to exercise the base-class dispatch."""

    type_name = "test_text"

    def _wire_serialize(self):
        return self._value

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(s)

    def _storage_serialize(self):
        blob = self._value.encode("utf-8")
        return [SerializedBlob(blob)], {"length": len(blob)}

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        return cls(blobs[0].decode("utf-8"))


def test_cannot_instantiate_abstract_base():
    with pytest.raises(TypeError):
        IOType()  # missing hook implementations


def test_wire_roundtrip():
    wire = _TextIOType("hi").serialize(format=WIRE)
    assert wire == "hi"
    assert _TextIOType.deserialize(wire, format=WIRE) == _TextIOType("hi")


def test_storage_roundtrip():
    blobs, meta = _TextIOType("hi").serialize(format=STORAGE)
    assert meta["length"] == 2
    raw = [b.value for b in blobs]
    assert _TextIOType.deserialize(raw, format=STORAGE) == _TextIOType("hi")


def test_default_format_is_storage():
    out = _TextIOType("hi").serialize()
    assert isinstance(out, tuple)  # (blobs, metadata)


def test_invalid_format_raises():
    with pytest.raises(ValueError):
        _TextIOType("hi").serialize(format="bogus")
    with pytest.raises(ValueError):
        _TextIOType.deserialize("hi", format="bogus")


def test_descriptor_has_no_value():
    assert _TextIOType().to_spec() == {"type": "test_text"}


def test_eq_and_hash():
    assert _TextIOType("x") == _TextIOType("x")
    assert _TextIOType("x") != _TextIOType("y")
    assert hash(_TextIOType("x")) == hash(_TextIOType("x"))
