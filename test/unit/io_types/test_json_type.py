from metaflow.io_types import Json


def test_json_wire_round_trip_dict():
    j = Json({"key": "value", "nested": [1, 2, 3]})
    s = j.serialize(format="wire")
    j2 = Json.deserialize(s, format="wire")
    assert j2.value == {"key": "value", "nested": [1, 2, 3]}


def test_json_wire_round_trip_list():
    j = Json([1, "two", None, True])
    s = j.serialize(format="wire")
    j2 = Json.deserialize(s, format="wire")
    assert j2.value == [1, "two", None, True]


def test_json_storage_round_trip():
    j = Json({"a": {"b": [1, 2]}, "c": None})
    blobs, meta = j.serialize(format="storage")
    assert len(blobs) == 1
    j2 = Json.deserialize([b.value for b in blobs], format="storage")
    assert j2.value == j.value


def test_json_to_spec():
    assert Json().to_spec() == {"type": "json"}


def test_json_empty_dict():
    j = Json({})
    blobs, _ = j.serialize(format="storage")
    j2 = Json.deserialize([b.value for b in blobs], format="storage")
    assert j2.value == {}


def test_json_empty_list():
    j = Json([])
    blobs, _ = j.serialize(format="storage")
    j2 = Json.deserialize([b.value for b in blobs], format="storage")
    assert j2.value == []


def test_json_deeply_nested():
    data = {"a": {"b": {"c": {"d": [1, 2, {"e": True}]}}}}
    j = Json(data)
    s = j.serialize(format="wire")
    j2 = Json.deserialize(s, format="wire")
    assert j2.value == data
