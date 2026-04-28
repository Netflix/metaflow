import importlib

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationMetadata,
    STORAGE,
    WIRE,
)
from metaflow.io_types.base import IOType, get_iotype_by_name


class IOTypeSerializer(ArtifactSerializer):
    """
    Bridge between :class:`IOType` and the pluggable serializer framework.

    Claims any :class:`IOType` instance on save. On load, reconstructs the
    original subclass by looking up its ``type_name`` in the global IOType
    registry (populated via :meth:`IOType.__init_subclass__`). The
    ``iotype_module`` / ``iotype_class`` hints stored in ``serializer_info``
    are kept as a secondary lookup path — useful when a subclass isn't yet
    registered in the reader process, or when inspecting artifacts produced
    by extensions whose code isn't installed locally.

    ``PRIORITY`` is 50 — ahead of the default (100) so this bridge catches
    :class:`IOType` artifacts before any generic catch-all, and always ahead
    of the :class:`PickleSerializer` fallback (9999).

    Only the ``STORAGE`` format is implemented on this bridge; ``WIRE`` is
    handled by callers that talk to :class:`IOType` directly (CLI parsing,
    protobuf payload construction), not through the datastore.
    """

    TYPE = "iotype"
    PRIORITY = 50

    _ENCODING_PREFIX = "iotype:"

    @classmethod
    def can_serialize(cls, obj):
        return isinstance(obj, IOType)

    @classmethod
    def can_deserialize(cls, metadata):
        return metadata.encoding.startswith(cls._ENCODING_PREFIX)

    @classmethod
    def serialize(cls, obj, format=STORAGE):
        if format == WIRE:
            raise NotImplementedError(
                "IOTypeSerializer only handles the STORAGE format; wire "
                "encoding is produced by calling IOType.serialize(format=WIRE) "
                "directly."
            )
        blobs, meta_dict = obj.serialize(format=STORAGE)
        size = sum(len(b.value) for b in blobs if isinstance(b.value, bytes))
        # Subclass metadata goes first so the routing keys below always win.
        # An IOType subclass whose ``_storage_serialize`` happens to return
        # ``iotype_module`` or ``iotype_class`` in its own meta dict must not
        # be able to overwrite the routing info the deserialize path needs.
        serializer_info = {
            **meta_dict,
            "iotype_module": obj.__class__.__module__,
            "iotype_class": obj.__class__.__name__,
        }
        return (
            blobs,
            SerializationMetadata(
                obj_type=obj.type_name,
                size=size,
                encoding=cls._ENCODING_PREFIX + obj.type_name,
                serializer_info=serializer_info,
            ),
        )

    @classmethod
    def deserialize(cls, data, metadata=None, format=STORAGE):
        if format == WIRE:
            raise NotImplementedError(
                "IOTypeSerializer only handles the STORAGE format."
            )
        info = metadata.serializer_info or {}
        # Primary path: registry lookup by the type_name encoded in the
        # artifact's encoding. Works whether or not the metadata service
        # propagates ``serializer_info`` to the reader.
        type_name = metadata.encoding[len(cls._ENCODING_PREFIX):]
        iotype_cls = get_iotype_by_name(type_name)
        # Fallback: explicit module/class hints in serializer_info. Useful for
        # inspecting artifacts produced by extensions not loaded locally.
        if iotype_cls is None and "iotype_module" in info and "iotype_class" in info:
            mod = importlib.import_module(info["iotype_module"])
            iotype_cls = getattr(mod, info["iotype_class"])
        if iotype_cls is None:
            raise ValueError(
                "IOTypeSerializer could not resolve a class for encoding %r; "
                "no IOType subclass is registered under type_name %r and "
                "serializer_info lacks iotype_module/iotype_class hints."
                % (metadata.encoding, type_name)
            )
        # Only allow actual IOType subclasses — metadata is untrusted input.
        if not (isinstance(iotype_cls, type) and issubclass(iotype_cls, IOType)):
            raise ValueError(
                "IOTypeSerializer resolved %r for encoding %r, which is not "
                "an IOType subclass" % (iotype_cls, metadata.encoding)
            )
        return iotype_cls.deserialize(data, format=STORAGE, metadata=info)
