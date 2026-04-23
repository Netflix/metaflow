import dataclasses
import json
import typing

from ..datastore.artifacts.serializer import SerializedBlob
from .base import IOType

# Implicit Python type -> IOType mapping for @dataclass field inference
PYTHON_TO_IOTYPE = {}  # populated after scalar imports to avoid circular deps


def _init_python_to_iotype():
    """Lazy init to avoid circular imports."""
    if PYTHON_TO_IOTYPE:
        return
    from .scalars import Bool, Float64, Int64, Text

    PYTHON_TO_IOTYPE.update(
        {
            str: Text,
            int: Int64,
            float: Float64,
            bool: Bool,
        }
    )


def _iotype_for_annotation(annotation):
    """
    Resolve a Python type annotation to an IOType class.

    Handles bare types (str, int, float, bool) via PYTHON_TO_IOTYPE.
    IOType subclasses pass through directly.
    """
    _init_python_to_iotype()
    if isinstance(annotation, type) and issubclass(annotation, IOType):
        return annotation
    iotype = PYTHON_TO_IOTYPE.get(annotation)
    if iotype is None:
        raise TypeError(
            "Cannot infer IOType for annotation %r. "
            "Use an explicit IOType (e.g., Json, List, Struct)." % (annotation,)
        )
    return iotype


class Struct(IOType):
    """
    Structured type mapping to Python @dataclass.

    Wire: JSON string. Storage: JSON UTF-8 bytes.

    Wraps a @dataclass instance. Fields are inferred from dataclass annotations
    with implicit scalar mapping (str->Text, int->Int64, float->Float64, bool->Bool).

    Parameters
    ----------
    value : dataclass instance or dict, optional
        The wrapped value. Dataclass instances are serialized via dataclasses.asdict.
        Plain dicts are serialized directly as JSON.
    dataclass_type : type, optional
        The @dataclass class for type descriptor use (no value).
    """

    type_name = "struct"

    def __init__(self, value=None, dataclass_type=None):
        if value is not None and dataclasses.is_dataclass(value):
            self._dataclass_type = type(value)
        elif dataclass_type is not None:
            self._dataclass_type = dataclass_type
        else:
            self._dataclass_type = None
        super().__init__(value)

    def _to_dict(self):
        """Convert value to dict, handling both dataclass and plain dict."""
        if dataclasses.is_dataclass(self._value):
            return dataclasses.asdict(self._value)
        if isinstance(self._value, dict):
            return self._value
        raise TypeError(
            "Struct value must be a dataclass instance or dict, got %s"
            % type(self._value).__name__
        )

    def _wire_serialize(self):
        return json.dumps(self._to_dict(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def _wire_deserialize(cls, s):
        return cls(json.loads(s))

    def _storage_serialize(self):
        blob = json.dumps(
            self._to_dict(), separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        meta = {}
        if self._dataclass_type is not None:
            meta["dataclass_module"] = self._dataclass_type.__module__
            meta["dataclass_class"] = self._dataclass_type.__name__
        return [SerializedBlob(blob)], meta

    @classmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        data = json.loads(blobs[0].decode("utf-8"))
        metadata = kwargs.get("metadata", {})
        dc_module = metadata.get("dataclass_module")
        dc_class = metadata.get("dataclass_class")
        if dc_module and dc_class:
            import importlib

            mod = importlib.import_module(dc_module)
            dc_type = getattr(mod, dc_class)
            # Security: only allow actual dataclasses, not arbitrary classes
            if not dataclasses.is_dataclass(dc_type):
                raise ValueError(
                    "Struct metadata references '%s.%s' which is not a dataclass"
                    % (dc_module, dc_class)
                )
            return cls(dc_type(**data), dataclass_type=dc_type)
        # Fallback: return as plain dict wrapped in Struct
        return cls(data)

    def to_spec(self):
        spec = {"type": self.type_name}
        if self._dataclass_type is not None and dataclasses.is_dataclass(
            self._dataclass_type
        ):
            # Use typing.get_type_hints() to resolve string annotations
            # (handles `from __future__ import annotations`)
            try:
                hints = typing.get_type_hints(self._dataclass_type)
            except Exception:
                hints = {}
            fields = []
            for f in dataclasses.fields(self._dataclass_type):
                annotation = hints.get(f.name, f.type)
                try:
                    field_iotype = _iotype_for_annotation(annotation)
                    field_spec = field_iotype().to_spec()
                except TypeError:
                    field_spec = {"type": str(annotation)}
                fields.append({"name": f.name, **field_spec})
            spec["fields"] = fields
        return spec
