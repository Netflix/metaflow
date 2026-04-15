from ..datastore.artifacts.serializer import SerializedBlob
from .base import IOType


class Enum(IOType):
    """
    Enum type — string value constrained to allowed values.

    Wire: string. Storage: UTF-8 bytes.

    Parameters
    ----------
    value : str, optional
        The enum value. Validated against allowed_values if provided.
    allowed_values : list of str
        The set of valid values for this enum.
    """

    type_name = "enum"

    def __init__(self, value=None, allowed_values=None):
        if allowed_values is not None:
            self._allowed_values = list(allowed_values)
        else:
            self._allowed_values = []
        if value is not None and self._allowed_values:
            if value not in self._allowed_values:
                raise ValueError(
                    "Enum value %r not in allowed values %r"
                    % (value, self._allowed_values)
                )
        super().__init__(value)

    def _wire_serialize(self):
        return str(self._value)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(s)

    def _storage_serialize(self):
        blob = str(self._value).encode("utf-8")
        meta = {}
        if self._allowed_values:
            meta["allowed_values"] = self._allowed_values
        return [SerializedBlob(blob)], meta

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        metadata = kwargs.get("metadata", {})
        allowed = metadata.get("allowed_values")
        return cls(blobs[0].decode("utf-8"), allowed_values=allowed)

    def to_spec(self):
        spec = {"type": self.type_name}
        if self._allowed_values:
            spec["allowed_values"] = self._allowed_values
        return spec

    def __repr__(self):
        from .base import _UNSET

        if self._value is _UNSET:
            return "Enum(allowed_values=%r)" % self._allowed_values
        return "Enum(%r, allowed_values=%r)" % (self._value, self._allowed_values)
