import pytest

from metaflow.io_types import Enum


def test_enum_wire_round_trip():
    e = Enum("red", allowed_values=["red", "green", "blue"])
    s = e.serialize(format="wire")
    assert s == "red"
    e2 = Enum.deserialize(s, format="wire")
    assert e2.value == "red"


def test_enum_storage_round_trip():
    e = Enum("green", allowed_values=["red", "green", "blue"])
    blobs, meta = e.serialize(format="storage")
    assert meta["allowed_values"] == ["red", "green", "blue"]
    e2 = Enum.deserialize(
        [b.value for b in blobs], format="storage", metadata=meta
    )
    assert e2.value == "green"
    assert e2._allowed_values == ["red", "green", "blue"]


def test_enum_validation():
    with pytest.raises(ValueError, match="not in allowed values"):
        Enum("yellow", allowed_values=["red", "green", "blue"])


def test_enum_no_allowed_values():
    e = Enum("anything")
    assert e.value == "anything"
    s = e.serialize(format="wire")
    assert s == "anything"


def test_enum_to_spec_with_values():
    e = Enum(allowed_values=["a", "b", "c"])
    spec = e.to_spec()
    assert spec == {"type": "enum", "allowed_values": ["a", "b", "c"]}


def test_enum_to_spec_without_values():
    e = Enum()
    assert e.to_spec() == {"type": "enum"}
