import importlib

from abc import ABCMeta, abstractmethod

from collections import namedtuple
from dataclasses import dataclass
from enum import IntEnum
from typing import (
    Dict,
    Generator,
    Generic,
    Iterator,
    List,
    ModuleType,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)

from metaflow.extension_support import get_package_by_name
from metaflow.metaflow_version import get_base_version
from metaflow.plugins import SERIALIZER_FINDERS


# Serializer priorities are as follows:
#  - a more specific serializer is preferred over a more generic one. This primarily
#    applies if a serializer uses ANY as the type of object it can serialize
#  - for two serializers of equal priority, the one added later is preferred.
#  - if two serializers have unequal priortiy, the one with the highest priority is
#    preferred.
# The order in which serializers are added depends on the order the SerializerFinders
# are called. They will be called in the order in which the extensions are loaded.
class SerializerPriority(IntEnum):
    FALLBACK = 0
    DEFAULT = 1
    PREFERRED = 2


ObjectType = TypeVar("ObjectType")
DataType = TypeVar("DataType")


class SerializerMeta(ABCMeta):
    def __new__(cls, name, bases, class_dict):
        serializer = super().__new__(cls, name, bases, class_dict)
        serializer.ID = serializer.__module__ + "." + serializer.__name__
        serializer.PROVIDED_BY = "unknown"

        # Extract ObjectType and DataType from generic parameters
        serializer.OBJECT_TYPE = None
        serializer.DATA_TYPE = None

        for base in bases:
            if get_origin(base) == Generic:
                args = get_args(base)
                if len(args) != 2:
                    raise ValueError(
                        "Serializer %s must have exactly two generic parameters"
                        % serializer.ID
                    )
                serializer.OBJECT_TYPE = args[0]
                serializer.DATA_TYPE = args[1]
                break
        if serializer.OBJECT_TYPE is None:
            raise ValueError(
                "Serializer %s must have a generic parameter indicating its type"
                % serializer.ID
            )

        return serializer


class Serializer(Generic[ObjectType, DataType], metaclass=SerializerMeta):
    """
    Abstract base class for single object de/serializers

    NOTE: This functionality is explicitly experimental and *explicitly not supported*.
    Unlike other not explicitly supported functionality that has remained stable
    for a while, this functionality is explicitly not supported at this time.
    That said, if you find it useful and have feedback, please let us know so we can
    try to take that into account in our future development.
    """

    PROVIDED_BY = "unknown"

    @abstractmethod
    def serialize(self, obj: ObjectType) -> DataType:
        """
        Serialize an object to a single data blob

        Parameters
        ----------
        obj : ObjectType
            The object to serialize

        Returns
        -------
        DataType
            The serialized data
        """
        pass

    @abstractmethod
    def deserialize(self, data: DataType) -> ObjectType:
        """
        Deserialize data back to an object

        Parameters
        ----------
        data : DataType
            The data to deserialize

        Returns
        -------
        ObjectType
            The deserialized object
        """
        pass


class StreamingSerializer(Generic[ObjectType, DataType], metaclass=SerializerMeta):
    """
    Abstract base class for streaming de/serializers

    NOTE: This functionality is explicitly experimental and *explicitly not supported*.
    Unlike other not explicitly supported functionality that has remained stable
    for a while, this functionality is explicitly not supported at this time.
    That said, if you find it useful and have feedback, please let us know so we can
    try to take that into account in our future development.
    """

    PROVIDED_BY = "unknown"

    @abstractmethod
    def serialize_stream(self, obj: ObjectType) -> Generator[DataType, None, None]:
        """
        Serialize an object to a stream of data chunks

        Parameters
        ----------
        obj : ObjectType
            The object to serialize

        Yields
        ------
        DataType
            Chunks of serialized data
        """
        yield  # pragma: no cover

    @abstractmethod
    def deserialize_stream(self, data_stream: Iterator[DataType]) -> ObjectType:
        """
        Deserialize a stream of data chunks back to an object

        Parameters
        ----------
        data_stream : Iterator[DataType]
            An iterator of data chunks to deserialize

        Returns
        -------
        ObjectType
            The deserialized object
        """
        pass


class SerializerFinder:
    FROM_EXTENSION = "unknown"

    @classmethod
    def get_serializer(
        cls, module_path: str, module: ModuleType
    ) -> Optional[List[Tuple[Serializer, SerializerPriority]]]:
        # Idea of this method is to figure out if there are types that are handled
        # by this finder and return the approriate serializers.
        return None

    @classmethod
    def load_serializer(
        cls, class_path: str, *args, **kwargs
    ) -> Union[Serializer, StreamingSerializer]:
        """
        Helper method to load a serializer specified through the class path

        This will properly load the serializer class and allow you to return it through
        get_serializer.

        Parameters
        ----------
        class_path : str
            Path to the serializer
        *args :
            Arguments to pass to the serializer constructor
        **kwargs :
            Keyword arguments to pass to the serializer constructor

        Returns
        -------
        Union[Serializer, StreamingSerializer]
            The serializer
        """
        path, cls_name = class_path.rsplit(".", 1)
        try:
            mod = importlib.import_module(path)
        except ImportError as e:
            raise ValueError(f"Cannot locate the serializer module at '{path}'") from e
        serializer_cls = getattr(mod, cls_name, None)
        if serializer_cls is None:
            raise ValueError(
                f"Cannot locate the serializer class '{cls_name}' in module '{path}'"
            )
        if not issubclass(serializer_cls, Serializer) and not issubclass(
            serializer_cls, StreamingSerializer
        ):
            raise ValueError(
                f"Serializer class '{cls_name}' in module '{path}' is neither a "
                "Serializer nor a StreamingSerializer"
            )
        if cls.FROM_EXTENSION == "metaflow":
            # This is base metaflow so we use the version we have from metaflow
            serializer_cls.PROVIDED_BY = f"metaflow@{get_base_version()}"
        elif cls.FROM_EXTENSION != "unknown":
            # This is from an extension so we try to get the version of the extension
            pkg = get_package_by_name(cls.FROM_EXTENSION)
            if pkg is not None:
                serializer_cls.PROVIDED_BY = f"{pkg.package_name}@{pkg.package_version}"
        return serializer_cls(*args, **kwargs)


@dataclass
class SerializerInfo:
    serializer: Optional[Union[Serializer, StreamingSerializer]] = None
    priority: bool = False


class GlobalSerializer:
    _serializer_mappings: Dict[type, Dict[Tuple[type, bool], SerializerInfo]] = {}
    _all_serializers: Dict[str, Union[Serializer, StreamingSerializer]] = {}

    @classmethod
    def serialize(
        cls, obj: object, serializer_id: Optional[str] = None
    ) -> Tuple[object, str]:
        """
        Serialize and object.

        To serialize an object, you typically do not need to specify a serializer_id
        but can if you want to override the one that would be automatically chosen based
        on the priority rules.

        Parameters
        ----------
        obj : object
            The object to serialize.
        transformer_id : str, optional, default None
            The ID of the serializer to use -- if not specified, the best available
            transformer will be used. If no transformer is found, an error will be raised.

        Returns
        -------
        Tuple[object, str]
            The transformed object as well as the serializer_id to be used when
            deserializing.
        """
        pass

    @classmethod
    def deserialize(cls, data: object, serializer_id: str) -> object:
        """
        Deserialize an object.

        Parameters
        ----------
        data : object
            The data to deserialize.
        transformer_id : str
            The ID of the serializer to use.

        Returns
        -------
        object
            The deserialized object.
        """
        pass

    @classmethod
    def stream_serialize(
        cls, obj: object, serializer_id: Optional[str] = None
    ) -> Generator[Tuple[object, str], None, None]:
        """
        Serialize an object returning a generator.

        Parameters
        ----------
        obj : object
            The object to serialize.
        serializer_id : str, optional, default None
            The ID of the serializer to use -- if not specified, the best available
            serializer will be used.

        Yields
        ------
        Tuple[object, str]
            Tuples of (serialized_chunk, serializer_id)
        """
        pass

    @classmethod
    def stream_deserialize(
        cls, data_iterator: Iterator[object], serializer_id: str
    ) -> object:
        """
        Deserialize from an iterator of data chunks.

        Parameters
        ----------
        data_iterator : Iterator[object]
            An iterator of data chunks to deserialize.
        serializer_id : str
            The ID of the serializer to use.

        Returns
        -------
        object
            The deserialized object.
        """
        pass

    @classmethod
    def new_module_callback(cls, module_path: str, new_module: ModuleType) -> None:
        """
        Callback when a new module is loaded. This is used to find any new serializer

        Parameters
        ----------
        module_path : str
            The path of the newly loaded module.
        new_module : ModuleType
            The newly loaded module.
        """
        # Go through the serializer finders to see if they have any new serializer to
        # register
        for finder in SERIALIZER_FINDERS:
            finder = cast(SerializerFinder, finder)
            new_serializers = finder.get_serializer(module_path, new_module)
            if new_serializers is not None:
                for serializer, serializer_priority in new_serializers:
                    has_classic = isinstance(serializer, Serializer)
                    has_streaming = isinstance(serializer, StreamingSerializer)
                    # Look for an existing serializer for this type
                    serializers_for_type = cls._serializer_mappings.setdefault(
                        serializer.OBJECT_TYPE, {}
                    )
                    if has_classic:
                        existing = cast(
                            Optional[SerializerInfo],
                            serializers_for_type.get(
                                (serializer.DATA_TYPE, False), None
                            ),
                        )
                        if existing and existing.priority <= serializer_priority:
                            # Replace if better or equal priority
                            existing.serializer = serializer
                            existing.priority = serializer_priority
                    if has_streaming:
                        existing = cast(
                            Optional[SerializerInfo],
                            serializers_for_type.get(
                                (serializer.DATA_TYPE, True), None
                            ),
                        )
                        if existing and existing.priority <= serializer_priority:
                            # Replace if better or equal priority
                            existing.serializer = serializer
                            existing.priority = serializer_priority
                    # Add to _all_serializers so we can look-up the reverse mapping
                    # when deserializing
                    cls._all_serializers[serializer.ID] = serializer
