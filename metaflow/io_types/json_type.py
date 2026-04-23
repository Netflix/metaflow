import json

from ..datastore.artifacts.serializer import SerializedBlob
from .base import IOType, _UNSET


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

    def __hash__(self):
        # ``_value`` is typically a dict or list (unhashable), so the base
        # class ``hash((type, _value))`` raises TypeError. Hash the canonical
        # JSON representation instead — stable across equal values and
        # consistent with ``__eq__`` (which compares ``_value`` directly:
        # equal dicts/lists produce identical sorted-key JSON).
        if self._value is _UNSET:
            return hash((type(self), _UNSET))
        return hash((type(self), self._wire_serialize()))
