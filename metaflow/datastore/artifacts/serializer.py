from abc import abstractmethod
from collections import namedtuple


SerializationMetadata = namedtuple(
    "SerializationMetadata", ["type", "size", "encoding", "serializer_info"]
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
    compress_method : str
        Compression method for new blobs. Ignored for references. Default "gzip".
    """

    def __init__(self, value, is_reference=None, compress_method="gzip"):
        if not isinstance(value, (str, bytes)):
            raise TypeError(
                "SerializedBlob value must be str or bytes, got %s" % type(value).__name__
            )
        self.value = value
        self.compress_method = compress_method
        if is_reference is None:
            self.is_reference = isinstance(value, str)
        else:
            self.is_reference = is_reference

    @property
    def needs_save(self):
        """True if this blob contains new bytes that need to be stored."""
        return not self.is_reference


class SerializerStore(type):
    """
    Metaclass for ArtifactSerializer that auto-registers subclasses by TYPE.

    Provides deterministic ordering: serializers are sorted by (PRIORITY, registration_order).
    Lower PRIORITY values are tried first. Registration order breaks ties.
    """

    _all_serializers = {}
    _registration_order = []

    def __init__(cls, name, bases, namespace):
        super().__init__(name, bases, namespace)
        if cls.TYPE is not None:
            if cls.TYPE not in SerializerStore._all_serializers:
                SerializerStore._registration_order.append(cls.TYPE)
            SerializerStore._all_serializers[cls.TYPE] = cls

    @staticmethod
    def get_ordered_serializers():
        """
        Return serializer classes sorted by (PRIORITY, registration_order).

        This ordering is deterministic for a given set of loaded serializers.
        """
        order = SerializerStore._registration_order
        return sorted(
            SerializerStore._all_serializers.values(),
            key=lambda s: (s.PRIORITY, order.index(s.TYPE)),
        )


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
    def can_serialize(cls, obj):
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
    def can_deserialize(cls, metadata):
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
    def serialize(cls, obj):
        """
        Serialize obj to blobs and metadata. Must be side-effect-free.

        Parameters
        ----------
        obj : Any
            The Python object to serialize.

        Returns
        -------
        tuple
            (List[SerializedBlob], SerializationMetadata)
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def deserialize(cls, blobs, metadata, context):
        """
        Deserialize blobs back to a Python object.

        Parameters
        ----------
        blobs : List[bytes]
            The raw blob data.
        metadata : SerializationMetadata
            Metadata stored alongside the artifact.
        context : Any
            Optional context for deserialization (e.g., task vs client loading).

        Returns
        -------
        Any
        """
        raise NotImplementedError
