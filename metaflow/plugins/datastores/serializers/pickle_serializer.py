import pickle

from typing import Any, Dict, List, Optional, Tuple, Union

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializedBlob,
    SerializationMetadata,
)


class PickleSerializer(ArtifactSerializer):
    """
    A serializer that uses Python's pickle module to serialize artifacts.
    """

    TYPE = "pickle"

    @classmethod
    def can_deserialize(cls, metadata: SerializationMetadata) -> bool:
        # Support any possible pickle version we ever used

        return metadata.encoding in [
            "pickle-v4",
            "pickle-v2",
            "gzip+pickle-v4",
            "gzip+pickle-v2",
        ]

    @classmethod
    def can_serialize(cls, obj: Any) -> bool:
        # By default, we can serialize everything. This serializer should be tried last
        return True

    @classmethod
    def serialize(cls, obj: Any) -> Tuple[List[SerializedBlob], SerializationMetadata]:

        # Moved to pickle v4 by default as Metaflow only supports 3.5+
        pickled_obj = pickle.dumps(obj, protocol=4)
        return (
            [SerializedBlob(pickled_obj, is_reference=False, compress_method="gzip")],
            SerializationMetadata(str(type(obj)), len(pickled_obj), "pickle-v4", {}),
        )

    @classmethod
    def deserialize(
        cls, blobs: List[bytes], metadata: SerializationMetadata, context: Any
    ) -> Any:
        if len(blobs) != 1:
            raise ValueError("PickleSerializer expects exactly one blob")
        return pickle.loads(blobs[0])
