import pickle

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationMetadata,
    SerializedBlob,
)


class PickleSerializer(ArtifactSerializer):
    """
    Default serializer using Python's pickle module.

    This is the universal fallback — can_serialize always returns True.
    PRIORITY is set to 9999 so custom serializers are always tried first.
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
    def serialize(cls, obj):
        blob = pickle.dumps(obj, protocol=4)
        encoding = "pickle-v4"
        return (
            [SerializedBlob(blob, is_reference=False, compress_method="gzip")],
            SerializationMetadata(
                type=str(type(obj)),
                size=len(blob),
                encoding=encoding,
                serializer_info={},
            ),
        )

    @classmethod
    def deserialize(cls, blobs, metadata, context):
        return pickle.loads(blobs[0])
