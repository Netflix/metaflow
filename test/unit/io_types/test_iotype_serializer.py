"""Tests for the IOTypeSerializer bridge between IOType and the datastore."""

import dataclasses

import pytest

from metaflow.datastore.artifacts.serializer import (
    SerializationMetadata,
    WIRE,
)
from metaflow.io_types import Json, Struct
from metaflow.plugins.datastores.serializers.iotype_serializer import IOTypeSerializer


@dataclasses.dataclass
class _Config:
    threshold: float
    name: str


# ---------------------------------------------------------------------------
# can_serialize / can_deserialize
# ---------------------------------------------------------------------------


def test_can_serialize_iotype_instances():
    assert IOTypeSerializer.can_serialize(Json({"a": 1})) is True
    assert IOTypeSerializer.can_serialize(Struct(_Config(0.5, "x"))) is True


def test_cannot_serialize_plain_python():
    assert IOTypeSerializer.can_serialize({"a": 1}) is False
    assert IOTypeSerializer.can_serialize("hello") is False
    assert IOTypeSerializer.can_serialize(42) is False


def test_can_deserialize_iotype_prefix():
    meta = SerializationMetadata(
        obj_type="json", size=0, encoding="iotype:json", serializer_info={}
    )
    assert IOTypeSerializer.can_deserialize(meta) is True


def test_cannot_deserialize_non_iotype_encoding():
    meta = SerializationMetadata(
        obj_type="dict", size=0, encoding="pickle-v4", serializer_info={}
    )
    assert IOTypeSerializer.can_deserialize(meta) is False


# ---------------------------------------------------------------------------
# serialize — metadata shape
# ---------------------------------------------------------------------------


def test_serialize_produces_iotype_encoding():
    blobs, meta = IOTypeSerializer.serialize(Json({"x": 1}))
    assert meta.encoding == "iotype:json"
    assert meta.obj_type == "json"
    assert meta.serializer_info["iotype_module"] == "metaflow.io_types.json_type"
    assert meta.serializer_info["iotype_class"] == "Json"
    assert len(blobs) == 1


def test_serialize_wire_format_not_supported():
    with pytest.raises(NotImplementedError):
        IOTypeSerializer.serialize(Json({"x": 1}), format=WIRE)
    with pytest.raises(NotImplementedError):
        IOTypeSerializer.deserialize(b"{}", format=WIRE)


# ---------------------------------------------------------------------------
# Round-trip: serialize -> deserialize
# ---------------------------------------------------------------------------


def test_json_roundtrip():
    original = Json({"threshold": 0.5, "name": "x", "n": 3})
    blobs, meta = IOTypeSerializer.serialize(original)
    raw = [b.value for b in blobs]
    result = IOTypeSerializer.deserialize(raw, metadata=meta)
    assert isinstance(result, Json)
    assert result.value == original.value


def test_struct_roundtrip_preserves_dataclass():
    original = Struct(_Config(threshold=0.75, name="model"))
    blobs, meta = IOTypeSerializer.serialize(original)
    raw = [b.value for b in blobs]
    result = IOTypeSerializer.deserialize(raw, metadata=meta)
    assert isinstance(result, Struct)
    assert isinstance(result.value, _Config)
    assert result.value.threshold == 0.75
    assert result.value.name == "model"


# ---------------------------------------------------------------------------
# Security: deserialize refuses non-IOType classes
# ---------------------------------------------------------------------------


def test_deserialize_rejects_non_iotype_class():
    # Craft metadata pointing at a non-IOType class (e.g. json.JSONDecoder).
    meta = SerializationMetadata(
        obj_type="bogus",
        size=0,
        encoding="iotype:bogus",
        serializer_info={
            "iotype_module": "json",
            "iotype_class": "JSONDecoder",
        },
    )
    with pytest.raises(ValueError, match="not an IOType subclass"):
        IOTypeSerializer.deserialize([b"{}"], metadata=meta)


# ---------------------------------------------------------------------------
# serialize — routing-key precedence
# ---------------------------------------------------------------------------


def test_subclass_metadata_cannot_overwrite_routing_keys():
    """An IOType subclass whose _storage_serialize returns ``iotype_module`` or
    ``iotype_class`` in its own meta dict must not be able to overwrite the
    routing keys the bridge writes — otherwise deserialize dispatch is
    corrupted.
    """
    from metaflow.datastore.artifacts.serializer import SerializedBlob
    from metaflow.io_types.base import IOType

    class _Poisoner(IOType):
        type_name = "poisoner"

        def _wire_serialize(self):
            return ""

        @classmethod
        def _wire_deserialize(cls, s):
            return cls()

        def _storage_serialize(self):
            return [SerializedBlob(b"{}")], {
                "iotype_module": "attacker.module",
                "iotype_class": "AttackerClass",
            }

        @classmethod
        def _storage_deserialize(cls, blobs, **kwargs):
            return cls()

    _, meta = IOTypeSerializer.serialize(_Poisoner())
    assert meta.serializer_info["iotype_module"] == _Poisoner.__module__
    assert meta.serializer_info["iotype_class"] == "_Poisoner"
