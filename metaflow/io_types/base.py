from abc import ABCMeta, abstractmethod


_UNSET = object()


class IOType(object, metaclass=ABCMeta):
    """
    Base class for typed Metaflow artifacts.

    IOType serves dual purposes:
    - **Type descriptor** (no value): ``Int64`` describes an int64 field in a spec.
    - **Value wrapper** (with value): ``Int64(42)`` wraps a value for typed serialization.

    Both support ``to_spec()`` for JSON schema generation.

    Subclasses implement four internal operations unified behind a ``format``
    parameter:

    - ``format='wire'``: string-based (for CLI args, protobuf, external APIs)
    - ``format='storage'``: blob-based (for S3/disk persistence via datastore)

    Storage byte order is little-endian.
    """

    type_name = None  # e.g., "text", "json", "int64" — set by subclasses

    def __init__(self, value=_UNSET):
        self._value = value

    @property
    def value(self):
        """The wrapped Python value, or _UNSET if this is a descriptor only."""
        return self._value

    # --- Public API (UX sugar) ---

    def serialize(self, format="wire"):
        """
        Serialize the wrapped value.

        Parameters
        ----------
        format : str
            ``'wire'`` for string output, ``'storage'`` for blob output.

        Returns
        -------
        str or tuple
            Wire: a string. Storage: ``(List[SerializedBlob], dict)``.
        """
        if format == "wire":
            return self._wire_serialize()
        elif format == "storage":
            return self._storage_serialize()
        raise ValueError("format must be 'wire' or 'storage', got %r" % format)

    @classmethod
    def deserialize(cls, data, format="wire", **kwargs):
        """
        Deserialize data into an IOType instance.

        Parameters
        ----------
        data : str or List[bytes]
            Wire: a string. Storage: list of byte blobs.
        format : str
            ``'wire'`` or ``'storage'``.

        Returns
        -------
        IOType
        """
        if format == "wire":
            return cls._wire_deserialize(data)
        elif format == "storage":
            return cls._storage_deserialize(data, **kwargs)
        raise ValueError("format must be 'wire' or 'storage', got %r" % format)

    # --- Four internal operations (subclasses implement these) ---

    @abstractmethod
    def _wire_serialize(self):
        """Value -> string (for CLI args, protobuf, external APIs)."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _wire_deserialize(cls, s):
        """String -> IOType instance."""
        raise NotImplementedError

    @abstractmethod
    def _storage_serialize(self):
        """Value -> (List[SerializedBlob], metadata_dict). Side-effect-free."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def _storage_deserialize(cls, blobs, **kwargs):
        """(List[bytes], metadata) -> IOType instance."""
        raise NotImplementedError

    # --- Spec generation ---

    def to_spec(self):
        """JSON type spec. Works with or without a wrapped value."""
        return {"type": self.type_name}

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
