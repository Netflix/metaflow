from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from metaflow import FlowSpec
    from .serializer import ArtifactSerializer
    from ..task_datastore import TaskDatastore


class MetaflowArtifact:
    """
    Represents a special type of Metaflow Artifact that allows the Artifact to have
    some say in how it is serialized and deserialized as well as modify how it is
    presented to users.

    NOTE: This functionality is explicitly experimental and *explicitly not supported*.
    Unlike other not explicitly supported functionality that has remained stable
    for a while, this functionality is explicitly not supported at this time.
    That said, if you find it useful and have feedback, please let us know so we can
    try to take that into account in our future development.
    """

    def pre_persist(
        self, name: str, flow: "FlowSpec", datastore: "TaskDatastore"
    ) -> bool:
        """
        This method is called before any artifact is persisted to the datastore.

        All other artifacts that are going to be persisted are accessible through
        `flow`. This method can be used to perform any action before the artifact is
        persisted.

        Parameters
        ----------
        name : str
            The name of this artifact
        flow : FlowSpec
            The flow that the artifact is a part of
        datastore : TaskDatastore
            The datastore used for this task. This is also the datastore into which
            the artifact will be persisted.

        Returns
        -------
        bool
            True if the artifact should be persisted, False otherwise
        """
        return True

    def get_serializer(self) -> Optional[Union["ArtifactSerializer", str]]:
        """
        This method is called to determine how the artifact should be serialized:
          - If a string is returned, it is assumed to be the name of a serializer and
            and error is raised if the serializer is not found.
          - If None is returned, the default serializer resolution is used (in other
            words, the serializer used is determined by the first serializer that says
            it can serialize this artifact)
          - If an ArtifactSerializer is returned, that serializer is used to serialize

        Returns
        -------
        Optional[Union[ArtifactSerializer, str]]
            Serializer to use
        """
        return None

    def post_serialize(self, metadata: Dict[str, Any]) -> bool:
        """
        This method is called after the artifact has been serialized but not yet saved.

        This method should return whether or not post_persist needs to be called on this
        artifact. To limit memory consumption, once an artifact is serialized, it is
        typically destroyed. If the artifact needs to be made aware of the result of
        saving artifacts to the datastore (ie: the keys that it was stored at), return
        True to keep this artifact in memory until the end of persist. In that case, it
        is recommended to "hollow-out" the artifact (no value needed for serialization
        )

        Parameters
        ----------
        metadata : Dict[str, Any]
            Metadata returned by the serializer for this artifact

        Returns
        -------
        bool
            True if post_persist should be called on this artifact, False otherwise
        """
        return False

    def post_persist(
        self, metadata: Dict[str, Dict[str, Any]], mappings: Dict[str, List[str]]
    ) -> None:
        """
        This method is called after all artifacts have been persisted to the datastore.

        It is passed information returned by the persist process namely:
          - metadata: A dictionary of metadata returned by the serializer for
            each artifact. The dictionary is keyed by the name of the artifact.
          - mappings: A dictionary of keys where the artifact was persisted. The
            dictionary is keyed by the name of the artifact. An artifact can potentially
            be persisted using multiple files in the datastore.

        To ensure this function is called, `post_serialize` should return True.

        Parameters
        ----------
        metadata : Dict[str, Dict[str, Any]]
            Metadata returned by the serializer
        mappings : Dict[str, List[str]]
            List of keys where the artifact was persisted
        """
        return None

    def get_representation(self, name: str) -> Any:
        """
        This method is called to determine how the artifact should be represented to the user.

        Parameters
        ----------
        name : str
            The name of this artifact.

        Returns
        -------
        Any
            Representation of the artifact
        """
        return self

    def update_value(self, value: Any) -> None:
        """
        This method is called to update the value of the artifact.

        Parameters
        ----------
        value : Any
            Value to update the artifact with
        """
        pass
