import pytest

np = pytest.importorskip("numpy")

from metaflow.io_types import Tensor


def test_tensor_wire_round_trip():
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    t = Tensor(arr)
    s = t.serialize(format="wire")
    assert "|" in s  # header|base64
    t2 = Tensor.deserialize(s, format="wire")
    np.testing.assert_array_equal(t2.value, arr)


def test_tensor_storage_round_trip():
    arr = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.int32)
    t = Tensor(arr)
    blobs, meta = t.serialize(format="storage")
    assert meta["dtype"] in ("<i4", "int32")  # dtype.str includes endianness prefix
    assert meta["shape"] == [2, 3]
    t2 = Tensor.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    np.testing.assert_array_equal(t2.value, arr)
    assert t2.value.dtype == np.int32


def test_tensor_storage_float32():
    arr = np.array([3.14, 2.72], dtype=np.float32)
    t = Tensor(arr)
    blobs, meta = t.serialize(format="storage")
    assert meta["dtype"] in ("<f4", "float32")  # dtype.str includes endianness prefix
    assert len(blobs[0].value) == 8  # 2 * 4 bytes
    t2 = Tensor.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    np.testing.assert_array_almost_equal(t2.value, arr)


def test_tensor_3d():
    arr = np.ones((2, 3, 4), dtype=np.float64)
    t = Tensor(arr)
    blobs, meta = t.serialize(format="storage")
    assert meta["shape"] == [2, 3, 4]
    t2 = Tensor.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    assert t2.value.shape == (2, 3, 4)
    np.testing.assert_array_equal(t2.value, arr)


def test_tensor_1d_single_element():
    arr = np.array([42], dtype=np.int64)
    t = Tensor(arr)
    blobs, meta = t.serialize(format="storage")
    assert meta["shape"] == [1]
    t2 = Tensor.deserialize([b.value for b in blobs], format="storage", metadata=meta)
    np.testing.assert_array_equal(t2.value, arr)


def test_tensor_to_spec_with_value():
    arr = np.zeros((10, 5), dtype=np.float32)
    t = Tensor(arr)
    spec = t.to_spec()
    assert spec["type"] == "tensor"
    assert spec["dtype"] == "float32"
    assert spec["shape"] == [10, 5]


def test_tensor_to_spec_without_value():
    t = Tensor()
    assert t.to_spec() == {"type": "tensor"}


def test_tensor_equality():
    a = np.array([1, 2, 3])
    assert Tensor(a) == Tensor(a.copy())
    assert Tensor(np.array([1])) != Tensor(np.array([2]))


def test_tensor_wire_preserves_dtype():
    for dtype in [np.float32, np.float64, np.int32, np.int64]:
        arr = np.array([1, 2, 3], dtype=dtype)
        t = Tensor(arr)
        s = t.serialize(format="wire")
        t2 = Tensor.deserialize(s, format="wire")
        assert t2.value.dtype == dtype


def test_tensor_storage_little_endian():
    """Storage serialization always produces little-endian bytes."""
    arr = np.array([1], dtype=np.int32)
    t = Tensor(arr)
    blobs, meta = t.serialize(format="storage")
    # Little-endian int32 1 = 0x01000000
    assert blobs[0].value == b"\x01\x00\x00\x00"
    # dtype.str should indicate little-endian
    assert meta["dtype"].startswith("<") or meta["dtype"].startswith("|")


def test_tensor_not_hashable():
    """Tensor with ndarray value should not be hashable (like numpy arrays)."""
    t = Tensor(np.array([1, 2, 3]))
    with pytest.raises(TypeError):
        hash(t)
