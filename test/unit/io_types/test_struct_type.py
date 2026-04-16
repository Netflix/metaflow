import dataclasses
from dataclasses import dataclass

import pytest

from metaflow.io_types import Struct


@dataclass
class SimpleData:
    name: str
    count: int
    score: float
    active: bool


@dataclass
class NestedData:
    label: str
    sub: dict  # not auto-inferred, but works with JSON serde


def test_struct_wire_round_trip():
    s = Struct(SimpleData(name="test", count=5, score=3.14, active=True))
    wire = s.serialize(format="wire")
    s2 = Struct.deserialize(wire, format="wire")
    # Wire deserializes to dict (no dataclass type info in wire format)
    assert s2.value == {"name": "test", "count": 5, "score": 3.14, "active": True}


def test_struct_storage_round_trip():
    original = SimpleData(name="test", count=5, score=3.14, active=True)
    s = Struct(original)
    blobs, meta = s.serialize(format="storage")
    assert "dataclass_module" in meta
    assert "dataclass_class" in meta
    s2 = Struct.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    assert s2.value == original
    assert type(s2.value) is SimpleData


def test_struct_storage_without_dataclass_type():
    """When metadata lacks dataclass info, falls back to dict."""
    s = Struct(SimpleData(name="x", count=1, score=0.0, active=False))
    blobs, _ = s.serialize(format="storage")
    s2 = Struct.deserialize([b.value for b in blobs], format="storage", metadata={})
    assert isinstance(s2.value, dict)
    assert s2.value["name"] == "x"


def test_struct_to_spec():
    s = Struct(dataclass_type=SimpleData)
    spec = s.to_spec()
    assert spec["type"] == "struct"
    assert len(spec["fields"]) == 4
    field_names = [f["name"] for f in spec["fields"]]
    assert field_names == ["name", "count", "score", "active"]
    # Check implicit mapping
    name_field = next(f for f in spec["fields"] if f["name"] == "name")
    assert name_field["type"] == "text"
    count_field = next(f for f in spec["fields"] if f["name"] == "count")
    assert count_field["type"] == "int64"


def test_struct_to_spec_no_dataclass():
    s = Struct()
    assert s.to_spec() == {"type": "struct"}


def test_struct_nested_data():
    nd = NestedData(label="test", sub={"key": [1, 2, 3]})
    s = Struct(nd)
    blobs, meta = s.serialize(format="storage")
    s2 = Struct.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    assert s2.value == nd


def test_struct_wire_deserialize_then_reserialize():
    """Wire round-trip: deserialize returns dict, re-serialize should work on dict."""
    s = Struct(SimpleData(name="test", count=5, score=3.14, active=True))
    wire = s.serialize(format="wire")
    s2 = Struct.deserialize(wire, format="wire")
    # s2 wraps a dict — re-serializing should work
    wire2 = s2.serialize(format="wire")
    s3 = Struct.deserialize(wire2, format="wire")
    assert s3.value == s2.value


def test_struct_security_rejects_non_dataclass():
    """Metadata pointing to a non-dataclass class should be rejected."""
    blobs = [b'{"cmd": "echo pwned"}']
    meta = {"dataclass_module": "subprocess", "dataclass_class": "Popen"}
    with pytest.raises(ValueError, match="not a dataclass"):
        Struct.deserialize(blobs, format="storage", metadata=meta)


def test_struct_plain_dict_value():
    """Struct wrapping a plain dict works for serde."""
    s = Struct({"x": 1, "y": "hello"})
    wire = s.serialize(format="wire")
    s2 = Struct.deserialize(wire, format="wire")
    assert s2.value == {"x": 1, "y": "hello"}
