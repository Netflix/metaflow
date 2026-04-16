from metaflow.io_types import List, Map, Text, Int64


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


def test_list_wire_round_trip():
    l = List([1, 2, 3])
    s = l.serialize(format="wire")
    l2 = List.deserialize(s, format="wire")
    assert l2.value == [1, 2, 3]


def test_list_storage_round_trip():
    l = List(["a", "b", "c"], element_type=Text)
    blobs, meta = l.serialize(format="storage")
    assert meta["element_type"] == "text"
    l2 = List.deserialize([b.value for b in blobs], format="storage")
    assert l2.value == ["a", "b", "c"]


def test_list_to_spec():
    l = List(element_type=Text)
    spec = l.to_spec()
    assert spec == {"type": "list", "element_type": {"type": "text"}}


def test_list_to_spec_no_element_type():
    l = List()
    assert l.to_spec() == {"type": "list"}


def test_list_nested():
    l = List([[1, 2], [3, 4]])
    blobs, _ = l.serialize(format="storage")
    l2 = List.deserialize([b.value for b in blobs], format="storage")
    assert l2.value == [[1, 2], [3, 4]]


def test_list_empty():
    l = List([])
    blobs, _ = l.serialize(format="storage")
    l2 = List.deserialize([b.value for b in blobs], format="storage")
    assert l2.value == []


def test_list_mixed_types():
    l = List([1, "two", None, True])
    s = l.serialize(format="wire")
    l2 = List.deserialize(s, format="wire")
    assert l2.value == [1, "two", None, True]


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------


def test_map_wire_round_trip():
    m = Map({"a": 1, "b": 2})
    s = m.serialize(format="wire")
    m2 = Map.deserialize(s, format="wire")
    assert m2.value == {"a": 1, "b": 2}


def test_map_storage_round_trip():
    m = Map({"x": 10, "y": 20}, key_type=Text, value_type=Int64)
    blobs, meta = m.serialize(format="storage")
    assert meta["key_type"] == "text"
    assert meta["value_type"] == "int64"
    m2 = Map.deserialize([b.value for b in blobs], format="storage")
    assert m2.value == {"x": 10, "y": 20}


def test_map_to_spec():
    m = Map(key_type=Text, value_type=Int64)
    spec = m.to_spec()
    assert spec == {
        "type": "map",
        "key_type": {"type": "text"},
        "value_type": {"type": "int64"},
    }


def test_map_to_spec_no_types():
    m = Map()
    assert m.to_spec() == {"type": "map"}


def test_map_nested_values():
    m = Map({"a": {"nested": [1, 2]}, "b": {"nested": [3]}})
    blobs, _ = m.serialize(format="storage")
    m2 = Map.deserialize([b.value for b in blobs], format="storage")
    assert m2.value == {"a": {"nested": [1, 2]}, "b": {"nested": [3]}}


def test_map_empty():
    m = Map({})
    blobs, _ = m.serialize(format="storage")
    m2 = Map.deserialize([b.value for b in blobs], format="storage")
    assert m2.value == {}
