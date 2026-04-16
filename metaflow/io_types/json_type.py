import json

from ..datastore.artifacts.serializer import SerializedBlob
from .base import IOType


class Json(IOType):
    """JSON type (dict or list). Wire: JSON string. Storage: UTF-8 JSON bytes."""

    type_name = "json"

    def _wire_serialize(self):
        return json.dumps(self._value, separators=(",", ":"), sort_keys=True)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(json.loads(s))

    def _storage_serialize(self):
        blob = json.dumps(self._value, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        return [SerializedBlob(blob)], {}

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        return cls(json.loads(blobs[0].decode("utf-8")))
