import inspect
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from enum import Enum
from typing import Any, List, Optional, Tuple, Union


class SerializationFormat(str, Enum):
    """
    Serialization format for :class:`ArtifactSerializer`.

    ``STORAGE`` produces ``(blobs, metadata)`` for the datastore persist path;
    ``WIRE`` produces a ``str`` for CLI args, protobuf payloads, and
    cross-process IPC.

    This subclasses ``str`` so that existing equality checks and JSON / artifact
    metadata round-trips continue to work with the underlying ``"storage"`` /
    ``"wire"`` values.
    """

    STORAGE = "storage"
    WIRE = "wire"


SerializationMetadata = namedtuple(
    "SerializationMetadata", ["obj_type", "size", "encoding", "serializer_info"]
)


class SerializedBlob(object):
    """
    Represents a single blob produced by a serializer.

    A serializer may produce multiple blobs per artifact. Each blob is either:
    - New bytes to be stored (is_reference=False, value is bytes)
    - A reference to already-stored data (is_reference=True, value is a string key)

    Parameters
    ----------
    value : Union[str, bytes]
        The blob data (bytes) or a reference key (str).
    is_reference : bool, optional
        If None, auto-detected from value type: str -> reference, bytes -> new data.
    """

    def __init__(self, value, is_reference=None):
        if not isinstance(value, (str, bytes)):
            raise TypeError(
                "SerializedBlob value must be str or bytes, got %s"
                % type(value).__name__
            )
        self.value = value
        if is_reference is None:
            self.is_reference = isinstance(value, str)
        else:
            self.is_reference = is_reference

    @property
    def needs_save(self):
        """True if this blob contains new bytes that need to be stored."""
        return not self.is_reference


class SerializerStore(ABCMeta):
    """
    Metaclass for ArtifactSerializer that auto-registers subclasses by TYPE.

    Provides deterministic ordering: serializers are sorted by (PRIORITY, registration_order).
    Lower PRIORITY values are tried first. Registration order breaks ties.
    """

    _all_serializers = {}
    _ordered_cache = None
    # Lazy-registry classes we have already materialized (imported and folded
    # into the dispatch order). Tracking them separately lets
    # ``get_ordered_serializers`` skip the cache rebuild when no new lazy
    # configs have become loadable since the last call.
    _materialized_lazy_classes = []
    _materialized_lazy_types = set()

    def __init__(cls, name, bases, namespace):
        super().__init__(name, bases, namespace)
        # Skip the abstract base and any subclass that didn't implement all
        # abstract methods — registering a partially-abstract class would
        # blow up only at dispatch time.
        if cls.TYPE is None or inspect.isabstract(cls):
            return
        SerializerStore._all_serializers[cls.TYPE] = cls
        SerializerStore._ordered_cache = None

    @staticmethod
    def get_ordered_serializers():
        """
        Return serializer classes sorted by (PRIORITY, registration_order).

        Python 3.7+ dicts preserve insertion order, so enumerating
        ``_all_serializers.values()`` yields registration order. A stable sort
        on PRIORITY preserves that tiebreaker.

        Serializers registered via the lazy registry are materialized here
        too: each registered class is imported on demand and folded into the
        dispatch order. Already-materialized lazy classes are remembered so
        that steady-state calls (no new lazy classes became importable) hit
        the cached ordered list — otherwise a single lazy config would force
        a rebuild on every call.
        """
        # Imported locally to avoid a circular import between this module and
        # ``lazy_registry`` (which depends on the ArtifactSerializer ABC).
        from .lazy_registry import iter_registered_configs, load_serializer_class

        newly_materialized = False
        for cfg in iter_registered_configs():
            if cfg.canonical_type in SerializerStore._materialized_lazy_types:
                continue
            cls = load_serializer_class(cfg.canonical_type)
            if cls is None:
                # Target module not imported yet; try again next call.
                continue
            SerializerStore._materialized_lazy_types.add(cfg.canonical_type)
            SerializerStore._materialized_lazy_classes.append(cls)
            newly_materialized = True

        if SerializerStore._ordered_cache is None or newly_materialized:
            # De-duplicate: lazy classes typically also self-register via the
            # metaclass, but when loaded outside normal import flow they may
            # not. ``dict.fromkeys`` preserves first-seen order while dropping
            # duplicates.
            combined = list(
                dict.fromkeys(
                    list(SerializerStore._all_serializers.values())
                    + SerializerStore._materialized_lazy_classes
                )
            )
            SerializerStore._ordered_cache = sorted(combined, key=lambda s: s.PRIORITY)
        return SerializerStore._ordered_cache


class ArtifactSerializer(object, metaclass=SerializerStore):
    """
    Abstract base class for artifact serializers.

    Subclasses must set TYPE to a unique string identifier and implement
    all four class methods. Subclasses are auto-registered by the SerializerStore
    metaclass on class definition.

    Attributes
    ----------
    TYPE : str or None
        Unique identifier for this serializer (e.g., "pickle", "iotype").
        Set to None in the base class to prevent registration.
    PRIORITY : int
        Dispatch priority. Lower values are tried first. Default 100.
        PickleSerializer uses 9999 as the universal fallback.
    """

    TYPE = None
    PRIORITY = 100

    @classmethod
    @abstractmethod
    def can_serialize(cls, obj: Any) -> bool:
        """
        Return True if this serializer can handle the given object.

        Parameters
        ----------
        obj : Any
            The Python object to serialize.

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def can_deserialize(cls, metadata: SerializationMetadata) -> bool:
        """
        Return True if this serializer can deserialize given the metadata.

        Parameters
        ----------
        metadata : SerializationMetadata
            Metadata stored alongside the artifact.

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def serialize(
        cls,
        obj: Any,
        format: SerializationFormat = SerializationFormat.STORAGE,
    ) -> Union[Tuple[List["SerializedBlob"], SerializationMetadata], str]:
        """
        Serialize obj. Must be side-effect-free: this method may be invoked
        multiple times (caching, retries, parallel dispatch) and must not
        perform I/O, mutate global state, or register the object elsewhere.
        Side effects that need to happen at persist time belong in hooks,
        not in the serializer.

        Parameters
        ----------
        obj : Any
            The Python object to serialize.
        format : SerializationFormat
            Either ``SerializationFormat.STORAGE`` (default) or
            ``SerializationFormat.WIRE``.
            - ``STORAGE`` returns a tuple ``(List[SerializedBlob], SerializationMetadata)``
              for persisting through the datastore.
            - ``WIRE`` returns a ``str`` representation for CLI args, protobuf
              payloads, and cross-process IPC. Serializers that cannot provide
              a wire encoding should raise ``NotImplementedError``.

        Returns
        -------
        tuple or str
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def deserialize(
        cls,
        data: Union[List[bytes], str],
        metadata: Optional[SerializationMetadata] = None,
        format: SerializationFormat = SerializationFormat.STORAGE,
    ) -> Any:
        """
        Deserialize back to a Python object.

        Parameters
        ----------
        data : Union[List[bytes], str]
            ``List[bytes]`` when ``format=STORAGE``; ``str`` when ``format=WIRE``.
        metadata : SerializationMetadata, optional
            Metadata stored alongside the artifact. Required for STORAGE,
            ignored for WIRE.
        format : SerializationFormat
            Either ``SerializationFormat.STORAGE`` (default) or
            ``SerializationFormat.WIRE``.

        Returns
        -------
        Any
        """
        raise NotImplementedError
