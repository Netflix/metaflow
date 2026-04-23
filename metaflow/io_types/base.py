"""Typed-artifact contract for Metaflow.

This module defines the minimal :class:`IOType` abstract base class. OSS
Metaflow ships the contract; concrete types (scalars, tensors, enums,
dataclass-backed structs, etc.) live in extensions — they embody
deployment-specific opinions about encoding, byte order, and dataclass
inference that do not belong in core.

:class:`IOType` mirrors the ``format`` argument introduced on
:class:`metaflow.datastore.artifacts.serializer.ArtifactSerializer` so a
single subclass can own both representations:

- ``STORAGE`` — blob-based, persisted through the datastore.
- ``WIRE`` — string-based, for CLI args, protobuf payloads, and
  cross-process IPC.

Subclasses implement four hooks (``_wire_serialize``, ``_wire_deserialize``,
``_storage_serialize``, ``_storage_deserialize``); callers use the public
``serialize(format=...)`` / ``deserialize(data, format=...)`` methods.
"""

from abc import ABCMeta, abstractmethod

from metaflow.datastore.artifacts.serializer import STORAGE, WIRE


_UNSET = object()

# Registry of concrete IOType subclasses keyed by their ``type_name``. Populated
# by ``IOType.__init_subclass__``. The datastore encodes each artifact's
# ``type_name`` in its ``SerializationMetadata.encoding`` (``iotype:<name>``),
# so ``IOTypeSerializer.deserialize`` can recover the class without the
# metadata service having to persist the Python module+class path.
_TYPE_REGISTRY = {}


def get_iotype_by_name(type_name):
    """Return the IOType subclass registered under ``type_name``, or None."""
    return _TYPE_REGISTRY.get(type_name)


def _make_hashable(value):
    """
    Recursively convert a JSON-like value to a hashable form.

    dicts -> frozenset of (key, hashable(value)) pairs.
    lists -> tuple of hashable elements.
    Everything else assumed hashable (int, float, bool, str, None, ...).

    Using ``frozenset``/``tuple`` preserves Python's numeric equivalence
    (``1 == 1.0 == True`` -> equal hashes) that ``json.dumps`` would
    otherwise break by rendering each as a distinct string. This keeps the
    ``__eq__`` / ``__hash__`` contract intact when IOType subclasses delegate
    value equality to the wrapped Python object.
    """
    if isinstance(value, dict):
        return frozenset((k, _make_hashable(v)) for k, v in value.items())
    if isinstance(value, list):
        return tuple(_make_hashable(v) for v in value)
    return value


class IOType(object, metaclass=ABCMeta):
    """
    Base class for typed Metaflow artifacts.

    An :class:`IOType` instance plays two roles:

    - **Descriptor** (no value): ``Int64`` in a spec describes an int64
      field.
    - **Wrapper** (with value): ``Int64(42)`` wraps a value for typed
      serialization.

    Subclasses implement four internal operations, dispatched by the
    ``format`` argument of the public :meth:`serialize` / :meth:`deserialize`
    methods.
    """

    type_name = None  # e.g. "text", "json", "int64" — set by subclasses.

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Register concrete subclasses so IOTypeSerializer can recover them
        # from just the ``type_name`` suffix of a stored artifact's encoding.
        # Abstract intermediates (``type_name`` is still None) don't register.
        # Last-write-wins: production code is expected to declare each
        # ``type_name`` on exactly one class; test-local subclasses that reuse
        # a name harmlessly overwrite each other.
        if cls.type_name:
            _TYPE_REGISTRY[cls.type_name] = cls

    def __init__(self, value=_UNSET):
        self._value = value

    @property
    def value(self):
        """The wrapped Python value, or ``_UNSET`` if this is a pure descriptor."""
        return self._value

    # -- Public API --------------------------------------------------------

    def serialize(self, format=STORAGE):
        """
        Serialize the wrapped value. Must be side-effect-free.

        Parameters
        ----------
        format : str
            ``STORAGE`` (default) returns ``(List[SerializedBlob], dict)``.
            ``WIRE`` returns a ``str``.
        """
        if format == WIRE:
            return self._wire_serialize()
        if format == STORAGE:
            return self._storage_serialize()
        raise ValueError("format must be %r or %r, got %r" % (STORAGE, WIRE, format))

    @classmethod
    def deserialize(cls, data, format=STORAGE, **kwargs):
        """
        Reconstruct an :class:`IOType` from serialized data.

        Parameters
        ----------
        data : Union[str, List[bytes]]
            ``str`` when ``format=WIRE``; ``List[bytes]`` when ``format=STORAGE``.
        format : str
            ``STORAGE`` (default) or ``WIRE``.
        **kwargs
            Forwarded to the underlying ``_storage_deserialize`` hook
            (e.g. metadata the datastore produced at save time).
        """
        if format == WIRE:
            return cls._wire_deserialize(data)
        if format == STORAGE:
            return cls._storage_deserialize(data, **kwargs)
        raise ValueError("format must be %r or %r, got %r" % (STORAGE, WIRE, format))

    # -- Subclass hooks ----------------------------------------------------

    @abstractmethod
    def _wire_serialize(self):
        """Value -> string (for CLI args, protobuf, external APIs)."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _wire_deserialize(cls, s):
        """String -> :class:`IOType` instance."""
        raise NotImplementedError

    @abstractmethod
    def _storage_serialize(self):
        """Value -> ``(List[SerializedBlob], metadata_dict)``. Side-effect-free."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        """``(List[bytes], metadata)`` -> :class:`IOType` instance."""
        raise NotImplementedError

    # -- Spec generation ---------------------------------------------------

    def to_spec(self):
        """JSON type spec. Works with or without a wrapped value."""
        return {"type": self.type_name}

    # -- Dunder ------------------------------------------------------------

    def __repr__(self):
        if self._value is _UNSET:
            return "%s()" % self.__class__.__name__
        return "%s(%r)" % (self.__class__.__name__, self._value)

    def __eq__(self, other):
        if type(self) is not type(other):
            return NotImplemented
        return self._value == other._value

    def __hash__(self):
        return hash((type(self), self._value))
