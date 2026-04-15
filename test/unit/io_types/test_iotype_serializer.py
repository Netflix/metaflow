"""
End-to-end integration tests for IOTypeSerializer bridging IOType to
the pluggable serializer framework via TaskDataStore.
"""

import os
from dataclasses import dataclass

import pytest

from metaflow.datastore.artifacts.serializer import SerializerStore
from metaflow.io_types import Bool, Enum, Float64, Int32, Int64, Json, List, Map, Text
from metaflow.io_types.struct_type import Struct
from metaflow.plugins.datastores.serializers.iotype_serializer import IOTypeSerializer
from metaflow.plugins.datastores.serializers.pickle_serializer import PickleSerializer


# ---------------------------------------------------------------------------
# IOTypeSerializer unit tests
# ---------------------------------------------------------------------------


def test_can_serialize_iotype():
    assert IOTypeSerializer.can_serialize(Text("hello")) is True
    assert IOTypeSerializer.can_serialize(Int64(42)) is True
    assert IOTypeSerializer.can_serialize(Json({"a": 1})) is True


def test_cannot_serialize_plain_python():
    assert IOTypeSerializer.can_serialize("hello") is False
    assert IOTypeSerializer.can_serialize(42) is False
    assert IOTypeSerializer.can_serialize({"a": 1}) is False


def test_can_deserialize_iotype_encoding():
    from metaflow.datastore.artifacts.serializer import SerializationMetadata

    meta = SerializationMetadata("text", 5, "iotype:text", {})
    assert IOTypeSerializer.can_deserialize(meta) is True

    meta = SerializationMetadata("json", 10, "iotype:json", {})
    assert IOTypeSerializer.can_deserialize(meta) is True


def test_cannot_deserialize_pickle_encoding():
    from metaflow.datastore.artifacts.serializer import SerializationMetadata

    meta = SerializationMetadata("dict", 100, "pickle-v4", {})
    assert IOTypeSerializer.can_deserialize(meta) is False


def test_serialize_returns_correct_metadata():
    blobs, meta = IOTypeSerializer.serialize(Text("hello"))
    assert meta.encoding == "iotype:text"
    assert meta.type == "text"
    assert meta.serializer_info["iotype_class"] == "Text"
    assert "iotype_module" in meta.serializer_info


def test_round_trip_text():
    original = Text("hello world")
    blobs, meta = IOTypeSerializer.serialize(original)
    raw_blobs = [b.value for b in blobs]
    result = IOTypeSerializer.deserialize(raw_blobs, meta, context=None)
    assert isinstance(result, Text)
    assert result.value == "hello world"


def test_round_trip_int64():
    original = Int64(999)
    blobs, meta = IOTypeSerializer.serialize(original)
    raw_blobs = [b.value for b in blobs]
    result = IOTypeSerializer.deserialize(raw_blobs, meta, context=None)
    assert isinstance(result, Int64)
    assert result.value == 999


def test_round_trip_json():
    original = Json({"nested": [1, 2, {"deep": True}]})
    blobs, meta = IOTypeSerializer.serialize(original)
    raw_blobs = [b.value for b in blobs]
    result = IOTypeSerializer.deserialize(raw_blobs, meta, context=None)
    assert isinstance(result, Json)
    assert result.value == {"nested": [1, 2, {"deep": True}]}


def test_round_trip_bool():
    for val in [True, False]:
        original = Bool(val)
        blobs, meta = IOTypeSerializer.serialize(original)
        result = IOTypeSerializer.deserialize([b.value for b in blobs], meta, None)
        assert result.value is val


def test_round_trip_enum():
    original = Enum("red", allowed_values=["red", "green", "blue"])
    blobs, meta = IOTypeSerializer.serialize(original)
    result = IOTypeSerializer.deserialize([b.value for b in blobs], meta, None)
    assert isinstance(result, Enum)
    assert result.value == "red"


def test_round_trip_list():
    original = List([1, 2, 3], element_type=Int64)
    blobs, meta = IOTypeSerializer.serialize(original)
    result = IOTypeSerializer.deserialize([b.value for b in blobs], meta, None)
    assert isinstance(result, List)
    assert result.value == [1, 2, 3]


def test_round_trip_map():
    original = Map({"a": 1, "b": 2}, key_type=Text, value_type=Int64)
    blobs, meta = IOTypeSerializer.serialize(original)
    result = IOTypeSerializer.deserialize([b.value for b in blobs], meta, None)
    assert isinstance(result, Map)
    assert result.value == {"a": 1, "b": 2}


@dataclass
class _TestData:
    name: str
    value: int


def test_round_trip_struct():
    original = Struct(_TestData(name="test", value=42))
    blobs, meta = IOTypeSerializer.serialize(original)
    result = IOTypeSerializer.deserialize([b.value for b in blobs], meta, None)
    assert isinstance(result, Struct)
    assert result.value == _TestData(name="test", value=42)


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------


def test_deserialize_rejects_non_iotype_class():
    """Metadata pointing to a non-IOType class should be rejected."""
    from metaflow.datastore.artifacts.serializer import SerializationMetadata

    meta = SerializationMetadata(
        type="text",
        size=5,
        encoding="iotype:text",
        serializer_info={
            "iotype_module": "subprocess",
            "iotype_class": "Popen",
        },
    )
    with pytest.raises(ValueError, match="not an IOType subclass"):
        IOTypeSerializer.deserialize([b"hello"], meta, context=None)


def test_iotype_priority_before_pickle():
    assert IOTypeSerializer.PRIORITY < PickleSerializer.PRIORITY


def test_registered_in_store():
    assert "iotype" in SerializerStore._all_serializers


# ---------------------------------------------------------------------------
# TaskDataStore integration
# ---------------------------------------------------------------------------


@pytest.fixture
def task_datastore(tmp_path):
    from metaflow.datastore.flow_datastore import FlowDataStore
    from metaflow.plugins.datastores.local_storage import LocalStorage

    storage_root = str(tmp_path / "datastore")
    os.makedirs(storage_root, exist_ok=True)

    flow_ds = FlowDataStore(
        flow_name="TestFlow",
        environment=None,
        metadata=None,
        event_logger=None,
        monitor=None,
        storage_impl=LocalStorage,
        ds_root=storage_root,
    )

    task_ds = flow_ds.get_task_datastore(
        run_id="1",
        step_name="start",
        task_id="1",
        attempt=0,
        mode="w",
    )
    task_ds.init_task()
    # Use only IOTypeSerializer + PickleSerializer (isolate from test pollution)
    task_ds._serializers = [IOTypeSerializer, PickleSerializer]
    return task_ds


def test_iotype_through_datastore(task_datastore):
    """IOType artifacts go through IOTypeSerializer, plain objects through pickle."""
    artifacts = [
        ("typed_json", Json({"key": "value"})),
        ("typed_int", Int64(42)),
        ("plain_dict", {"key": "value"}),  # not wrapped in IOType
    ]
    task_datastore.save_artifacts(iter(artifacts))

    # IOType artifacts use iotype encoding
    assert task_datastore._info["typed_json"]["encoding"] == "iotype:json"
    assert task_datastore._info["typed_int"]["encoding"] == "iotype:int64"
    # Plain dict falls through to pickle
    assert task_datastore._info["plain_dict"]["encoding"] == "pickle-v4"

    # All round-trip correctly
    loaded = dict(task_datastore.load_artifacts(["typed_json", "typed_int", "plain_dict"]))
    assert isinstance(loaded["typed_json"], Json)
    assert loaded["typed_json"].value == {"key": "value"}
    assert isinstance(loaded["typed_int"], Int64)
    assert loaded["typed_int"].value == 42
    assert loaded["plain_dict"] == {"key": "value"}  # plain dict, not IOType


def test_mixed_iotypes_through_datastore(task_datastore):
    """Multiple IOType varieties in a single save/load cycle."""
    artifacts = [
        ("t", Text("hello")),
        ("b", Bool(True)),
        ("i32", Int32(100)),
        ("f64", Float64(3.14)),
        ("j", Json([1, 2, 3])),
        ("e", Enum("red", allowed_values=["red", "green"])),
        ("l", List([1, 2], element_type=Int64)),
    ]
    task_datastore.save_artifacts(iter(artifacts))
    loaded = dict(task_datastore.load_artifacts([name for name, _ in artifacts]))

    assert loaded["t"].value == "hello"
    assert loaded["b"].value is True
    assert loaded["i32"].value == 100
    assert loaded["f64"].value == 3.14
    assert loaded["j"].value == [1, 2, 3]
    assert loaded["e"].value == "red"
    assert loaded["l"].value == [1, 2]
