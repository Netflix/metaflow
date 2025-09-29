from dataclasses import dataclass


@dataclass(frozen=True)
class ArtifactBlob:
    """
    Represents the blob prior to compression to be stored by the datastore. The
    ArtifactBlob can optionally direct how the rest of the processing for this
    blob should be done (for example, compress or not, etc.)
    """

    value: bytes
    compress_method: str = "gzip"
