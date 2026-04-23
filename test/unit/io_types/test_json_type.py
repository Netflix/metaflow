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


def test_json_hashable_with_dict_value():
    """Json wrapping a dict must be hashable. The base class default
    ``hash((type(self), self._value))`` raises TypeError for unhashable
    values, so Json overrides it with a wire-format-based hash.
    """
    j = Json({"a": 1, "b": [2, 3]})
    # No TypeError.
    h = hash(j)
    assert isinstance(h, int)

    # Equal values must hash equal — even when insertion order differs.
    j2 = Json({"b": [2, 3], "a": 1})
    assert j == j2
    assert hash(j) == hash(j2)

    # Works inside a set.
    assert {Json({"x": 1}), Json({"x": 1})} == {Json({"x": 1})}


def test_json_hashable_with_list_value():
    j = Json([1, 2, 3])
    h = hash(j)
    assert isinstance(h, int)
    assert hash(Json([1, 2, 3])) == hash(j)
