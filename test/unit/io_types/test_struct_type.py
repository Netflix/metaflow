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


@dataclass
class WithInitFalse:
    a: int
    computed: int = dataclasses.field(init=False, default=0)

    def __post_init__(self):
        self.computed = self.a * 10


@dataclass(frozen=True)
class FrozenWithInitFalse:
    a: int
    computed: int = dataclasses.field(init=False, default=0)

    def __post_init__(self):
        # Frozen dataclasses must use object.__setattr__ to touch fields.
        object.__setattr__(self, "computed", self.a * 10)


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


def test_struct_init_false_field_round_trip():
    """Dataclasses with ``field(init=False)`` must round-trip without
    TypeError. ``dataclasses.asdict`` includes init=False fields in its
    output, but passing them to the generated ``__init__`` raises
    ``TypeError: __init__() got an unexpected keyword argument``. The
    reconstructor must skip them in kwargs and restore their values via
    ``setattr`` after construction.
    """
    original = WithInitFalse(a=5)
    assert original.computed == 50
    # Mutate the computed field after construction to prove the
    # serialized value (not __post_init__'s recomputation) survives.
    original.computed = 99

    s = Struct(original)
    blobs, meta = s.serialize(format="storage")
    s2 = Struct.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    assert type(s2.value) is WithInitFalse
    assert s2.value.a == 5
    assert s2.value.computed == 99


def test_struct_plain_dict_value():
    """Struct wrapping a plain dict works for serde."""
    s = Struct({"x": 1, "y": "hello"})
    wire = s.serialize(format="wire")
    s2 = Struct.deserialize(wire, format="wire")
    assert s2.value == {"x": 1, "y": "hello"}


def test_struct_frozen_dataclass_with_init_false_field_round_trip():
    """Frozen dataclasses reject plain ``setattr``. Reconstructing one with
    an ``init=False`` field must use ``object.__setattr__`` so the
    serialized value survives, matching how frozen dataclasses initialize
    such fields in their own ``__post_init__``.
    """
    original = FrozenWithInitFalse(a=7)
    assert original.computed == 70

    s = Struct(original)
    blobs, meta = s.serialize(format="storage")
    s2 = Struct.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    assert type(s2.value) is FrozenWithInitFalse
    assert s2.value.a == 7
    assert s2.value.computed == 70


def test_struct_hashable_with_unhashable_dataclass():
    """Struct wrapping a dataclass with mutable fields (list, dict) must be
    hashable. The base ``hash((type, _value))`` raises TypeError because
    dataclasses with mutable fields are unhashable; Struct overrides
    ``__hash__`` via ``_make_hashable``.
    """

    @dataclass
    class WithList:
        x: int
        items: list = dataclasses.field(default_factory=list)

    s = Struct(WithList(x=1, items=[1, 2, 3]))
    h = hash(s)
    assert isinstance(h, int)

    # Hash/eq contract: equal values hash equal.
    s2 = Struct(WithList(x=1, items=[1, 2, 3]))
    assert s == s2
    assert hash(s) == hash(s2)

    # Wrapping a plain dict also hashes without TypeError.
    s3 = Struct({"a": [1, 2], "b": {"nested": True}})
    assert isinstance(hash(s3), int)


def test_struct_hash_preserves_numeric_equivalence():
    """Same contract as Json: equal dicts with int/float/bool members must
    hash equal. Confirms the frozenset/tuple based ``_make_hashable`` is
    used (not ``json.dumps``, which would render ``1`` and ``1.0`` as
    distinct strings).
    """
    s_int = Struct({"x": 1})
    s_float = Struct({"x": 1.0})
    assert s_int == s_float
    assert hash(s_int) == hash(s_float)


def test_struct_descriptor_is_hashable():
    """Struct() (no value) must still be hashable via the _UNSET sentinel."""
    s = Struct()
    assert isinstance(hash(s), int)


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
