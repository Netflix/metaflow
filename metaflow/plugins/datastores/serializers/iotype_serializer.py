import importlib

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationMetadata,
)
from metaflow.io_types.base import IOType


class IOTypeSerializer(ArtifactSerializer):
    """
    Bridge between the IOType system and the pluggable serializer framework.

    Auto-detects IOType instances via isinstance check. On deserialization,
    reconstructs the original IOType subclass from metadata (module + class name).

    PRIORITY is 50: higher than default (100) but lower than domain-specific
    serializers. Always before PickleSerializer (9999).
    """

    TYPE = "iotype"
    PRIORITY = 50

    @classmethod
    def can_serialize(cls, obj):
        return isinstance(obj, IOType)

    @classmethod
    def can_deserialize(cls, metadata):
        return metadata.encoding.startswith("iotype:")

    @classmethod
    def serialize(cls, obj):
        blobs, meta_dict = obj.serialize(format="storage")
        return (
            blobs,
            SerializationMetadata(
                type=obj.type_name,
                size=sum(
                    len(b.value) for b in blobs if isinstance(b.value, bytes)
                ),
                encoding="iotype:%s" % obj.type_name,
                serializer_info={
                    "iotype_module": obj.__class__.__module__,
                    "iotype_class": obj.__class__.__name__,
                    **meta_dict,
                },
            ),
        )

    @classmethod
    def deserialize(cls, blobs, metadata, context):
        info = metadata.serializer_info
        mod = importlib.import_module(info["iotype_module"])
        iotype_cls = getattr(mod, info["iotype_class"])
        # Security: only allow actual IOType subclasses, not arbitrary classes
        if not (isinstance(iotype_cls, type) and issubclass(iotype_cls, IOType)):
            raise ValueError(
                "IOTypeSerializer metadata references '%s.%s' which is not an "
                "IOType subclass" % (info["iotype_module"], info["iotype_class"])
            )
        return iotype_cls.deserialize(blobs, format="storage", metadata=info)
