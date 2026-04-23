import base64
import json

from ..datastore.artifacts.serializer import SerializedBlob
from .base import IOType


def _to_little_endian(arr):
    """Ensure array is little-endian and contiguous."""
    np = _require_numpy()
    arr = np.ascontiguousarray(arr)
    if arr.dtype.byteorder == ">" or (
        arr.dtype.byteorder == "=" and import_sys_byteorder() == "big"
    ):
        arr = arr.byteswap().view(arr.dtype.newbyteorder("<"))
    return arr


def import_sys_byteorder():
    import sys

    return sys.byteorder


class Tensor(IOType):
    """
    N-dimensional array type backed by numpy ndarray.

    Wire: base64-encoded string (with shape/dtype in JSON prefix).
    Storage: raw little-endian bytes blob + shape/dtype metadata dict.

    All serialization normalizes to little-endian byte order regardless of
    the host platform's native endianness.

    Parameters
    ----------
    value : numpy.ndarray, optional
        The wrapped array value.
    """

    type_name = "tensor"

    def _wire_serialize(self):
        np = _require_numpy()
        arr = _to_little_endian(self._value)
        raw = arr.tobytes()
        header = json.dumps(
            {"dtype": arr.dtype.str, "shape": list(arr.shape)},
            separators=(",", ":"),
        )
        return header + "|" + base64.b64encode(raw).decode("ascii")

    @classmethod
    def _wire_deserialize(cls, s):
        np = _require_numpy()
        header_str, b64_data = s.split("|", 1)
        header = json.loads(header_str)
        raw = base64.b64decode(b64_data)
        arr = np.frombuffer(raw, dtype=np.dtype(header["dtype"])).reshape(
            header["shape"]
        )
        return cls(arr.copy())

    def _storage_serialize(self):
        np = _require_numpy()
        arr = _to_little_endian(self._value)
        blob = arr.tobytes()
        meta = {
            "dtype": arr.dtype.str,
            "shape": list(arr.shape),
        }
        return [SerializedBlob(blob)], meta

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        np = _require_numpy()
        metadata = kwargs.get("metadata", {})
        dtype = np.dtype(metadata["dtype"])
        shape = tuple(metadata["shape"])
        arr = np.frombuffer(blobs[0], dtype=dtype).reshape(shape)
        return cls(arr.copy())

    def to_spec(self):
        spec = {"type": self.type_name}
        if self._value is not None:
            try:
                import numpy as np

                if isinstance(self._value, np.ndarray):
                    spec["dtype"] = str(self._value.dtype)
                    spec["shape"] = list(self._value.shape)
            except ImportError:
                pass
        return spec

    def __eq__(self, other):
        if type(self) is not type(other):
            return NotImplemented
        try:
            import numpy as np

            if isinstance(self._value, np.ndarray) and isinstance(
                other._value, np.ndarray
            ):
                return (
                    self._value.shape == other._value.shape
                    and self._value.dtype == other._value.dtype
                    and np.array_equal(self._value, other._value)
                )
        except ImportError:
            pass
        return self._value == other._value

    # ndarray is not hashable — match numpy's behavior
    __hash__ = None


def _require_numpy():
    try:
        import numpy as np

        return np
    except ImportError:
        raise ImportError(
            "Tensor type requires numpy. Install it with: pip install numpy"
        )
