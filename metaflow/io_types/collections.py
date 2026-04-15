import json

from ..datastore.artifacts.serializer import SerializedBlob
from .base import IOType


class List(IOType):
    """
    Typed list. Wire: JSON string. Storage: JSON UTF-8 bytes.

    element_type is used for spec generation (to_spec) only. Serde uses JSON
    for the entire list — individual elements are not delegated to their IOType
    serializers. This is intentional: JSON is sufficient for list storage, and
    per-element typed serde can be added later if needed.

    Parameters
    ----------
    value : list, optional
        The wrapped list value.
    element_type : IOType subclass, optional
        The IOType class for list elements (for spec generation).
    """

    type_name = "list"

    def __init__(self, value=None, element_type=None):
        self._element_type = element_type
        super().__init__(value)

    def _wire_serialize(self):
        return json.dumps(self._value, separators=(",", ":"), sort_keys=True)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(json.loads(s))

    def _storage_serialize(self):
        blob = json.dumps(self._value, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        meta = {}
        if self._element_type is not None:
            meta["element_type"] = self._element_type.type_name
        return [SerializedBlob(blob)], meta

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        return cls(json.loads(blobs[0].decode("utf-8")))

    def to_spec(self):
        spec = {"type": self.type_name}
        if self._element_type is not None:
            spec["element_type"] = self._element_type().to_spec()
        return spec


class Map(IOType):
    """
    Typed map (dict with typed keys and values). Wire: JSON string. Storage: JSON UTF-8 bytes.

    key_type/value_type are used for spec generation (to_spec) only. Serde uses
    JSON for the entire map — individual entries are not delegated to their IOType
    serializers. Same rationale as List.

    Parameters
    ----------
    value : dict, optional
        The wrapped dict value.
    key_type : IOType subclass, optional
        The IOType class for map keys (for spec generation).
    value_type : IOType subclass, optional
        The IOType class for map values (for spec generation).
    """

    type_name = "map"

    def __init__(self, value=None, key_type=None, value_type=None):
        self._key_type = key_type
        self._value_type = value_type
        super().__init__(value)

    def _wire_serialize(self):
        return json.dumps(self._value, separators=(",", ":"), sort_keys=True)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(json.loads(s))

    def _storage_serialize(self):
        blob = json.dumps(self._value, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        meta = {}
        if self._key_type is not None:
            meta["key_type"] = self._key_type.type_name
        if self._value_type is not None:
            meta["value_type"] = self._value_type.type_name
        return [SerializedBlob(blob)], meta

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        return cls(json.loads(blobs[0].decode("utf-8")))

    def to_spec(self):
        spec = {"type": self.type_name}
        if self._key_type is not None:
            spec["key_type"] = self._key_type().to_spec()
        if self._value_type is not None:
            spec["value_type"] = self._value_type().to_spec()
        return spec
