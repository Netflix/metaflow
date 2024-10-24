import itertools
import pickle

from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING, Union

from metaflow import FlowSpec, step, current
from metaflow.datastore.artifacts import MetaflowArtifact
from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationMetadata,
    SerializedBlob,
)

if TYPE_CHECKING:
    from metaflow.datastore.task_datastore import TaskDatastore


# Example of a tracking artifact
class TrackedArtifact(MetaflowArtifact):
    TYPE_NAME = "tracked"

    def __init__(self, wrapped):
        self._wrapped = wrapped

    def get_serializer(self) -> Optional[Union[ArtifactSerializer, str]]:
        return None

    def get_representation(self, name) -> Any:
        self._name = name
        print("Artifact %s accessed by %s" % (self._name, current.pathspec))
        return self._wrapped

    def update_value(self, value: Any) -> None:
        print("Artfifact %s modified by %s" % (self._name, current.pathspec))
        self._wrapped = value


class HookDemoArtifact(TrackedArtifact):
    TYPE_NAME = "hook_demo"

    def pre_persist(
        self, name: str, flow: FlowSpec, datastore: Optional["TaskDatastore"]
    ) -> bool:
        self._name = name
        print(
            "Artifact %s is going to be persisted, looking for other MetaflowArtifacts"
            % self._name
        )
        for other_name, other_art in flow._orig_artifacts.items():
            if other_name == name:
                continue
            print(
                "Found artifact %s with value %s"
                % (other_name, other_art.get_representation(other_name))
            )
        if datastore:
            print(
                "Datastore already has the following artifacts: %s"
                % ", ".join(k for k, _ in datastore.items())
            )
        return True

    def get_serializer(self) -> Optional[Union[ArtifactSerializer, str]]:
        print("Getting serializer for %s" % self._name)
        return None

    def post_serialize(self, metadata: Dict[str, Any]) -> bool:
        print("Artifact %s has metadata %s" % (self._name, metadata))
        return True

    def post_persist(
        self, metadata: Dict[str, Dict[str, Any]], mappings: Dict[str, List[str]]
    ) -> None:
        print("Artifact %s was persisted to %s" % (self._name, mappings[self._name]))
        return None


class ChunkedList(HookDemoArtifact, ArtifactSerializer):
    TYPE_NAME = "chunked_list"
    TYPE = "chunked_list_serializer"

    def __init__(self, wrapped):
        self._start_idx = 0
        self._wrapped = wrapped

    # Artifact methods

    def get_serializer(self) -> Optional[Union[ArtifactSerializer, str]]:
        # We are our own serializer
        return self

    def pre_persist(
        self, name: str, flow: "FlowSpec", datastore: "TaskDatastore"
    ) -> bool:
        self._name = name
        # Check the blobs that are currently being used to persist this artifact
        if name in datastore:
            self._current_keys = datastore.keys_for_artifacts([name])[0]
            self._current_size = next(datastore.get_artifact_sizes([name]))[1]
        else:
            self._current_keys = []
            self._current_size = 0

    def get_representation(self, name: str) -> Any:
        return self._wrapped

    def update_value(self, value: Any) -> None:
        if len(value) < len(self._wrapped):
            raise ValueError("Chunked list is append-only")
        if any(x != y for x, y in zip(self._wrapped, value)):
            raise ValueError("Chunked list is append-only")
        if len(value) > len(self._wrapped):
            self._start_idx = len(self._wrapped)
        self._wrapped = value

    # Serializer methods

    @classmethod
    def can_deserialize(cls, metadata: SerializationMetadata) -> bool:
        return metadata.encoding == cls.TYPE

    def serialize(self, obj: Any) -> Tuple[List[SerializedBlob], SerializationMetadata]:
        blob_list = [
            SerializedBlob(value=k, is_reference=True) for k in self._current_keys
        ]
        if self._start_idx < len(self._wrapped):
            new_part = pickle.dumps(self._wrapped[self._start_idx :])
            blob_list.append(SerializedBlob(value=new_part))
            self._current_size += len(new_part)
        return blob_list, SerializationMetadata(
            type="list",
            size=self._current_size,
            encoding=self.TYPE,
            serializer_info={},
        )

    @classmethod
    def deserialize(
        cls, blobs: List[bytes], metadata: SerializationMetadata, context: Any
    ) -> Any:
        if metadata.encoding != cls.TYPE:
            raise ValueError("Invalid encoding for chunked list")
        c = cls(
            list(itertools.chain(*(pickle.loads(blobs[i]) for i in range(len(blobs)))))
        )
        c._start_idx = len(c._wrapped)
        return c


class ArtifactsFlow(FlowSpec):
    @step
    def start(self):
        self.x = TrackedArtifact(42)
        self.y = HookDemoArtifact(43)
        self.chunked_list = ChunkedList([1, 2, 3])
        self.next(self.hello)

    @step
    def hello(self):
        print("Going to access x: %s" % str(self.x))
        print("Got a list with elements: %s" % str(self.chunked_list))
        self.chunked_list.extend([4, 5, 6])
        self.x = 43
        self.next(self.end)

    @step
    def end(self):
        self.chunked_list.extend([1, 2, 3])
        print("X is now %s" % str(self.x))
        print("Hook demo is now %s" % str(self.y))
        print("List is now: %s" % str(self.chunked_list))


if __name__ == "__main__":
    ArtifactsFlow()
