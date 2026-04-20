import pickle

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationMetadata,
    SerializedBlob,
    STORAGE,
    WIRE,
)


class PickleSerializer(ArtifactSerializer):
    """
    Default serializer using Python's pickle module.

    This is the universal fallback — can_serialize always returns True.
    PRIORITY is set to 9999 so custom serializers are always tried first.
    Pickle produces binary bytes, so only the STORAGE format is supported;
    callers that need a wire representation should pick a serializer that
    implements it (e.g. an IOType- or JSON-based one in an extension).
    """

    TYPE = "pickle"
    PRIORITY = 9999

    _ENCODINGS = frozenset(
        ["pickle-v2", "pickle-v4", "gzip+pickle-v2", "gzip+pickle-v4"]
    )

    @classmethod
    def can_serialize(cls, obj):
        return True

    @classmethod
    def can_deserialize(cls, metadata):
        return metadata.encoding in cls._ENCODINGS

    @classmethod
    def serialize(cls, obj, format=STORAGE):
        if format == WIRE:
            raise NotImplementedError(
                "PickleSerializer does not support the WIRE format; pickle "
                "produces opaque binary bytes that are not safe to pass as "
                "CLI args or inline IPC payloads."
            )
        blob = pickle.dumps(obj, protocol=4)
        return (
            [SerializedBlob(blob, is_reference=False)],
            SerializationMetadata(
                obj_type=str(type(obj)),
                size=len(blob),
                encoding="pickle-v4",
                serializer_info={},
            ),
        )

    @classmethod
    def deserialize(cls, data, metadata=None, context=None, format=STORAGE):
        if format == WIRE:
            raise NotImplementedError(
                "PickleSerializer does not support the WIRE format."
            )
        return pickle.loads(data[0])
