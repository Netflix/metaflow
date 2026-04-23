import struct

import pytest

from metaflow.io_types import Bool, Float32, Float64, Int32, Int64, Text


# ---------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------


def test_text_wire_round_trip():
    t = Text("hello world")
    s = t.serialize(format="wire")
    assert s == "hello world"
    t2 = Text.deserialize(s, format="wire")
    assert t2.value == "hello world"


def test_text_storage_round_trip():
    t = Text("utf-8 test: cafe\u0301")
    blobs, meta = t.serialize(format="storage")
    assert len(blobs) == 1
    t2 = Text.deserialize([b.value for b in blobs], format="storage")
    assert t2.value == t.value


def test_text_to_spec():
    assert Text().to_spec() == {"type": "text"}


def test_text_empty_string():
    t = Text("")
    assert t.serialize(format="wire") == ""
    blobs, _ = t.serialize(format="storage")
    t2 = Text.deserialize([b.value for b in blobs], format="storage")
    assert t2.value == ""


# ---------------------------------------------------------------------------
# Bool
# ---------------------------------------------------------------------------


def test_bool_wire_round_trip():
    assert Bool(True).serialize(format="wire") == "true"
    assert Bool(False).serialize(format="wire") == "false"
    assert Bool.deserialize("true", format="wire").value is True
    assert Bool.deserialize("false", format="wire").value is False


def test_bool_wire_case_insensitive():
    assert Bool.deserialize("TRUE", format="wire").value is True
    assert Bool.deserialize("False", format="wire").value is False


def test_bool_wire_invalid():
    with pytest.raises(ValueError, match="true.*false"):
        Bool.deserialize("yes", format="wire")


def test_bool_storage_round_trip():
    for val in [True, False]:
        b = Bool(val)
        blobs, _ = b.serialize(format="storage")
        assert len(blobs[0].value) == 1
        b2 = Bool.deserialize([blobs[0].value], format="storage")
        assert b2.value is val


def test_bool_storage_rejects_invalid_bytes():
    with pytest.raises(ValueError, match="0x00 or 0x01"):
        Bool.deserialize([b"\x02"], format="storage")
    with pytest.raises(ValueError, match="0x00 or 0x01"):
        Bool.deserialize([b"\x00\x01"], format="storage")


def test_bool_to_spec():
    assert Bool().to_spec() == {"type": "bool"}


# ---------------------------------------------------------------------------
# Int32
# ---------------------------------------------------------------------------


def test_int32_wire_round_trip():
    i = Int32(42)
    s = i.serialize(format="wire")
    assert s == "42"
    i2 = Int32.deserialize(s, format="wire")
    assert i2.value == 42


def test_int32_storage_round_trip():
    i = Int32(-1000)
    blobs, _ = i.serialize(format="storage")
    assert len(blobs[0].value) == 4
    i2 = Int32.deserialize([blobs[0].value], format="storage")
    assert i2.value == -1000


def test_int32_storage_little_endian():
    i = Int32(1)
    blobs, _ = i.serialize(format="storage")
    assert blobs[0].value == b"\x01\x00\x00\x00"  # little-endian


def test_int32_range_check():
    Int32(2**31 - 1)  # max
    Int32(-(2**31))  # min
    with pytest.raises(ValueError, match="out of range"):
        Int32(2**31)
    with pytest.raises(ValueError, match="out of range"):
        Int32(-(2**31) - 1)


def test_int32_zero():
    i = Int32(0)
    blobs, _ = i.serialize(format="storage")
    i2 = Int32.deserialize([blobs[0].value], format="storage")
    assert i2.value == 0


def test_int32_to_spec():
    assert Int32().to_spec() == {"type": "int32"}


# ---------------------------------------------------------------------------
# Int64
# ---------------------------------------------------------------------------


def test_int64_wire_round_trip():
    i = Int64(2**40)
    s = i.serialize(format="wire")
    i2 = Int64.deserialize(s, format="wire")
    assert i2.value == 2**40


def test_int64_storage_round_trip():
    for val in [0, -1, 2**60, -(2**60)]:
        i = Int64(val)
        blobs, _ = i.serialize(format="storage")
        assert len(blobs[0].value) == 8
        i2 = Int64.deserialize([blobs[0].value], format="storage")
        assert i2.value == val


def test_int64_storage_little_endian():
    i = Int64(1)
    blobs, _ = i.serialize(format="storage")
    assert blobs[0].value == b"\x01\x00\x00\x00\x00\x00\x00\x00"


def test_int64_to_spec():
    assert Int64().to_spec() == {"type": "int64"}


# ---------------------------------------------------------------------------
# Float32
# ---------------------------------------------------------------------------


def test_float32_wire_round_trip():
    f = Float32(3.14)
    s = f.serialize(format="wire")
    f2 = Float32.deserialize(s, format="wire")
    assert abs(f2.value - 3.14) < 0.01  # float32 precision


def test_float32_storage_round_trip():
    f = Float32(1.5)  # exactly representable in float32
    blobs, _ = f.serialize(format="storage")
    assert len(blobs[0].value) == 4
    f2 = Float32.deserialize([blobs[0].value], format="storage")
    assert f2.value == 1.5


def test_float32_to_spec():
    assert Float32().to_spec() == {"type": "float32"}


# ---------------------------------------------------------------------------
# Float64
# ---------------------------------------------------------------------------


def test_float64_wire_round_trip():
    f = Float64(3.141592653589793)
    s = f.serialize(format="wire")
    f2 = Float64.deserialize(s, format="wire")
    assert f2.value == 3.141592653589793


def test_float64_storage_round_trip():
    f = Float64(2.718281828459045)
    blobs, _ = f.serialize(format="storage")
    assert len(blobs[0].value) == 8
    f2 = Float64.deserialize([blobs[0].value], format="storage")
    assert f2.value == 2.718281828459045


def test_float64_to_spec():
    assert Float64().to_spec() == {"type": "float64"}


# ---------------------------------------------------------------------------
# IOType base behavior
# ---------------------------------------------------------------------------


def test_repr_with_value():
    assert repr(Int64(42)) == "Int64(42)"
    assert repr(Text("hi")) == "Text('hi')"


def test_repr_without_value():
    assert repr(Int64()) == "Int64()"


def test_equality():
    assert Int64(42) == Int64(42)
    assert Int64(42) != Int64(43)
    assert Text("a") == Text("a")
    assert Text("a") != Int64(0)  # different types


def test_invalid_format():
    with pytest.raises(ValueError, match="format must be"):
        Int64(42).serialize(format="invalid")
    with pytest.raises(ValueError, match="format must be"):
        Int64.deserialize("42", format="invalid")
