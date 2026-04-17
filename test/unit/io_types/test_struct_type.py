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
class Inner:
    x: int
    y: str


@dataclass
class Outer:
    label: str
    inner: Inner


@dataclass
class NestedData:
    label: str
    sub: dict  # container — stays a dict on reconstruction


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


def test_struct_to_spec_default():
    """to_spec returns just the type name; richer schemas live in extensions."""
    assert Struct().to_spec() == {"type": "struct"}
    assert Struct(dataclass_type=SimpleData).to_spec() == {"type": "struct"}


def test_struct_nested_dataclass_roundtrip():
    """Directly nested @dataclass fields reconstruct to their original type."""
    original = Outer(label="root", inner=Inner(x=7, y="hi"))
    s = Struct(original)
    blobs, meta = s.serialize(format="storage")
    s2 = Struct.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    assert s2.value == original
    assert type(s2.value) is Outer
    assert type(s2.value.inner) is Inner


def test_struct_dict_field_stays_dict():
    """Container annotations like ``sub: dict`` aren't walked — dicts stay as-is."""
    nd = NestedData(label="test", sub={"key": [1, 2, 3]})
    s = Struct(nd)
    blobs, meta = s.serialize(format="storage")
    s2 = Struct.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    assert s2.value == nd
    assert type(s2.value) is NestedData


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


def test_struct_security_rejects_dataclass_instance():
    """A callable module-level *instance* that happens to be a dataclass must
    also be rejected — ``dataclasses.is_dataclass`` returns True for instances,
    so the guard has to require an actual class.
    """
    # Build a fake module in sys.modules with a callable dataclass instance.
    import sys
    import types

    @dataclass
    class _Sink:
        called_with: dict = dataclasses.field(default_factory=dict)

        def __call__(self, **kwargs):
            self.called_with.update(kwargs)
            return "attacker controlled"

    fake = types.ModuleType("_struct_security_probe")
    fake.sink_instance = _Sink()
    sys.modules["_struct_security_probe"] = fake
    try:
        meta = {
            "dataclass_module": "_struct_security_probe",
            "dataclass_class": "sink_instance",
        }
        blobs = [b'{"foo": "bar"}']
        with pytest.raises(ValueError, match="not a dataclass"):
            Struct.deserialize(blobs, format="storage", metadata=meta)
        # The callable instance must not have been invoked.
        assert fake.sink_instance.called_with == {}
    finally:
        sys.modules.pop("_struct_security_probe", None)


def test_struct_plain_dict_value():
    """Struct wrapping a plain dict works for serde."""
    s = Struct({"x": 1, "y": "hello"})
    wire = s.serialize(format="wire")
    s2 = Struct.deserialize(wire, format="wire")
    assert s2.value == {"x": 1, "y": "hello"}


def test_struct_descriptor_uses_unset_sentinel():
    """Struct() (no value) must behave as a pure descriptor via the same
    _UNSET sentinel the base IOType uses. Two empty descriptors should be
    equal, hashable, and ``repr`` cleanly — and must be distinguishable
    from ``Struct(None)`` which wraps an actual ``None`` value.
    """
    from metaflow.io_types.base import _UNSET

    a = Struct()
    b = Struct()
    assert a.value is _UNSET
    assert a == b
    assert hash(a) == hash(b)
    assert repr(a) == "Struct()"

    # Struct(None) is a wrapper around None and is NOT equivalent to a
    # descriptor — they compare by their wrapped value.
    c = Struct(None)
    assert c.value is None
    assert a != c
