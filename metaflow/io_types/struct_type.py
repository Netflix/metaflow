import dataclasses
import importlib
import json
import typing

from ..datastore.artifacts.serializer import SerializedBlob
from .base import IOType, _UNSET


def _reconstruct(dc_type, data):
    """
    Rebuild a dataclass instance from JSON-decoded ``data``, recursing into
    fields whose annotation is itself a dataclass. Containerized annotations
    (``List[Foo]``, ``Dict[str, Foo]``, ``Optional[Foo]``, ...) are left as
    raw JSON-decoded values; callers that need rich container reconstruction
    should wrap the field explicitly (e.g. in a ``List`` IOType shipped by
    an extension).
    """
    try:
        hints = typing.get_type_hints(dc_type)
    except Exception:
        hints = {}
    kwargs = {}
    for f in dataclasses.fields(dc_type):
        raw = data.get(f.name)
        annotation = hints.get(f.name, f.type)
        if (
            isinstance(annotation, type)
            and dataclasses.is_dataclass(annotation)
            and isinstance(raw, dict)
        ):
            kwargs[f.name] = _reconstruct(annotation, raw)
        else:
            kwargs[f.name] = raw
    return dc_type(**kwargs)


class Struct(IOType):
    """
    Structured type mapping to a Python ``@dataclass``.

    Wire: JSON string. Storage: JSON UTF-8 bytes.

    Wraps a ``@dataclass`` instance. On save, ``dataclasses.asdict`` flattens
    the whole tree to plain dicts; on load, fields typed as dataclasses are
    recursively rebuilt into their original types. Generic container
    annotations (``List[Foo]``, ``Dict[str, Foo]``, ``Optional[Foo]``) are
    not walked — those fields come back as raw JSON-decoded values. Wrap
    those explicitly (e.g. via ``List[Struct]`` support shipped by an
    extension) when you need typed containers.

    .. warning::
       ``Struct._storage_deserialize`` imports the dataclass module named in
       the artifact metadata. Metadata written by this class is safe, but
       metadata supplied from an untrusted source can trigger arbitrary
       imports (and any import-time side effects those modules carry).
       Only load artifacts from sources you trust.

    Parameters
    ----------
    value : dataclass instance or dict, optional
        The wrapped value. Dataclass instances are serialized via
        ``dataclasses.asdict``; plain dicts are serialized directly.
    dataclass_type : type, optional
        The ``@dataclass`` class, for type-descriptor use (no value).
    """

    type_name = "struct"

    def __init__(self, value=_UNSET, dataclass_type=None):
        if value is not _UNSET and dataclasses.is_dataclass(value):
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
            mod = importlib.import_module(dc_module)
            dc_type = getattr(mod, dc_class)
            # Guard against crafted metadata — require a class that's
            # actually a dataclass. ``dataclasses.is_dataclass`` alone
            # returns True for dataclass *instances*; the ``isinstance(..., type)``
            # check excludes that (and anything else callable).
            if not (isinstance(dc_type, type) and dataclasses.is_dataclass(dc_type)):
                raise ValueError(
                    "Struct metadata references '%s.%s' which is not a dataclass"
                    % (dc_module, dc_class)
                )
            return cls(_reconstruct(dc_type, data), dataclass_type=dc_type)
        # Fallback: return as plain dict wrapped in Struct
        return cls(data)
