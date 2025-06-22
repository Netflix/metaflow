from collections import namedtuple
from typing import Any, Dict, List, Optional, Tuple, Union

SerializationMetadata = namedtuple(
    "SerializationMetadata", "type size encoding serializer_info"
)


class SerializedBlob:

    def __init__(
        self,
        value: Union[str, bytes],
        is_reference: Optional[bool] = None,
        compress_method: str = "gzip",
    ):
        """
        Initializes a serialized blob

        Parameters
        ----------
        value : Union[str, bytes]
            The value of the serialized blob; a string will simply be stored as the
            reference to the already stored bytes in the content addressed store
            and bytes will be stored in the content addressed store.
        is_reference : bool, optional, default None
            If True or False, determines if the value passed is to be considered a
            reference to a previously saved blob or not. If None, the value is
            considered a reference if it is a string and not a reference if it is
            bytes.
        compress_method : str, optional, default "gzip"
            The compression method used for the serialized blob. Methods currently are
            "raw" (no compression) and "gzip" (gzip compression). Does not apply for
            values that are references only (ignored)
        """
        self._value = value
        if is_reference is None:
            self._is_blob = isinstance(value, bytes)
        else:
            self._is_blob = not is_reference
        if self._is_blob:
            self._compress_method = compress_method
        else:
            self._compress_method = None

    @property
    def do_save_blob(self) -> bool:
        """
        Returns True if the serialized blob should be saved as a blob

        Returns
        -------
        bool
            True if the serialized blob should be saved as a blob
        """
        return self._is_blob

    @property
    def value(self) -> Union[str, bytes]:
        """
        Returns the value of the serialized blob

        Returns
        -------
        Union[str, bytes]
            The value of the serialized blob; a string will simply be stored as the
            reference to the already stored bytes in the content addressed store
            and bytes will be stored in the content addressed store.
        """
        return self._value

    @property
    def compress_method(self) -> str:
        """
        Returns the compression method used for the serialized blob

        Returns
        -------
        str
            The compression method used for the serialized blob. Methods currently are
            "raw" (no compression) and "gzip" (gzip compression)
        """
        if self._compress_method is None:
            raise ValueError("No compression method for reference")
        return self._compress_method


class SerializerStore(type):
    _all_serializers = {}

    def __new__(cls, name, bases, class_dict):
        serializer = super().__new__(cls, name, bases, class_dict)
        if name != "ArtifactSerializer":  # Don't register the base class
            if name in cls._all_serializers:
                raise ValueError("Serializer with name %s already exists" % name)
            cls._all_serializers[name] = serializer
        return serializer

    def __getitem__(cls, key):
        try:
            return cls._all_serializers[key]
        except KeyError as e:
            raise KeyError("No serializer with name %s" % key) from e

    def __iter__(cls):
        return iter(cls._all_serializers.items())


class ArtifactSerializer(metaclass=SerializerStore):
    """
    Represents a de/serializer for artifacts

    NOTE: This functionality is explicitly experimental and *explicitly not supported*.
    Unlike other not explicitly supported functionality that has remained stable
    for a while, this functionality is explicitly not supported at this time.
    That said, if you find it useful and have feedback, please let us know so we can
    try to take that into account in our future development.
    """

    TYPE = None

    @classmethod
    def can_serialize(cls, obj: Any) -> bool:
        """
        Returns True if this serializer can serialize the given object

        Parameters
        ----------
        obj : Any
            Object to serialize

        Returns
        -------
        bool
            True if this serializer can serialize the object
        """
        return False

    @classmethod
    def can_deserialize(cls, metadata: SerializationMetadata) -> bool:
        """
        Returns True if this serializer can deserialize the object given the
        serialized metadata

        Parameters
        ----------
        metadata : SerializationMetadata
            Metadata that is returned by the serialize method

        Returns
        -------
        bool
            True if this ArtifactSerializer can deserialize the object based on the
            metadata provided
        """
        return False

    @classmethod
    def serialize(cls, obj: Any) -> Tuple[List[SerializedBlob], SerializationMetadata]:
        """
        Serialize the object passed in. Returns the serialized blob representation as
        well as additional metadata.

        Note that a single object can be serialized to multiple blobs, some of which
        may already exist in the datastore (and are therefore not stored again). If this
        is known a-priori, the serializer can let the system know by returning a
        reference blob (a string) instead of the actual bytes.

        Each returned blob can also determine how it wants to be compressed (if at all).

        Parameters
        ----------
        obj : Any
            The object to serialize

        Returns
        -------
        Tuple[List[SerializedBlob], SerializationMetadata]
            The serialized representation of the object as well as serialization
            metadata
        """
        raise NotImplementedError

    @classmethod
    def deserialize(
        cls, blobs: List[bytes], metadata: SerializationMetadata, context: Any
    ) -> Any:
        """
        Deserialize the object from the serialized representation. The blobs passed in
        will be in the same order as those returned by serialize. The metadata will be
        all the keys returned by deserialize, including "type", "size" and "encode_type".

        Parameters
        ----------
        blobs : List[bytes]
            The serialized representation of the object
        metadata : SerializationMetadata
            The metadata associated with the serialized object
        context : Any
            TODO: This is the context in which the deserialization is happening. It
            is meant to represent whether it is from a task, the client or the client
            within a task.

        Returns
        -------
        Any
            The deserialized object
        """
        raise NotImplementedError
