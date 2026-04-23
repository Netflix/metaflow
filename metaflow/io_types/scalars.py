import struct

from ..datastore.artifacts.serializer import SerializedBlob
from .base import IOType


class Text(IOType):
    """String type. Wire: identity. Storage: UTF-8 bytes."""

    type_name = "text"

    def _wire_serialize(self):
        return str(self._value)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(s)

    def _storage_serialize(self):
        blob = str(self._value).encode("utf-8")
        return [SerializedBlob(blob)], {}

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        return cls(blobs[0].decode("utf-8"))


class Bool(IOType):
    """Boolean type. Wire: "true"/"false". Storage: 1 byte (0/1)."""

    type_name = "bool"

    def _wire_serialize(self):
        return "true" if self._value else "false"

    @classmethod
    def _wire_deserialize(cls, s):
        if s.lower() == "true":
            return cls(True)
        elif s.lower() == "false":
            return cls(False)
        raise ValueError("Bool expects 'true' or 'false', got %r" % s)

    def _storage_serialize(self):
        blob = b"\x01" if self._value else b"\x00"
        return [SerializedBlob(blob)], {}

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        if len(blobs[0]) != 1 or blobs[0] not in (b"\x00", b"\x01"):
            raise ValueError(
                "Bool storage expects exactly 1 byte (0x00 or 0x01), got %r"
                % blobs[0]
            )
        return cls(blobs[0] == b"\x01")


class Int32(IOType):
    """32-bit signed integer. Wire: str(int). Storage: 4-byte little-endian."""

    type_name = "int32"

    _MIN = -(2**31)
    _MAX = 2**31 - 1

    def __init__(self, value=None):
        if value is not None and not (self._MIN <= value <= self._MAX):
            raise ValueError(
                "Int32 value %d out of range [%d, %d]" % (value, self._MIN, self._MAX)
            )
        super().__init__(value)

    def _wire_serialize(self):
        return str(self._value)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(int(s))

    def _storage_serialize(self):
        blob = struct.pack("<i", self._value)
        return [SerializedBlob(blob)], {}

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        return cls(struct.unpack("<i", blobs[0])[0])


class Int64(IOType):
    """64-bit signed integer. Wire: str(int). Storage: 8-byte little-endian."""

    type_name = "int64"

    def _wire_serialize(self):
        return str(self._value)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(int(s))

    def _storage_serialize(self):
        blob = struct.pack("<q", self._value)
        return [SerializedBlob(blob)], {}

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        return cls(struct.unpack("<q", blobs[0])[0])


class Float32(IOType):
    """32-bit IEEE 754 float. Wire: str(float). Storage: 4-byte little-endian."""

    type_name = "float32"

    def _wire_serialize(self):
        return str(self._value)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(float(s))

    def _storage_serialize(self):
        blob = struct.pack("<f", self._value)
        return [SerializedBlob(blob)], {}

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        # Round-trip through float32 to match precision
        return cls(struct.unpack("<f", blobs[0])[0])


class Float64(IOType):
    """64-bit IEEE 754 float. Wire: str(float). Storage: 8-byte little-endian."""

    type_name = "float64"

    def _wire_serialize(self):
        return str(self._value)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(float(s))

    def _storage_serialize(self):
        blob = struct.pack("<d", self._value)
        return [SerializedBlob(blob)], {}

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        return cls(struct.unpack("<d", blobs[0])[0])
