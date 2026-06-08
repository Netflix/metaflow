"""
Integration tests for the pluggable serializer framework wired into TaskDataStore.

Tests that:
- PickleSerializer handles standard Python objects through save/load_artifacts
- Custom serializers take priority over PickleSerializer
- Backward compat: old artifacts (without serializer_info) still load
- Metadata includes serializer_info when present
"""

import json
import threading

import pytest

from metaflow.datastore.artifacts.diagnostic import (
    SerializerRecord,
    SerializerState,
)
from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationFormat,
    SerializationMetadata,
    SerializedBlob,
    SerializerStore,
)
from metaflow.datastore.exceptions import (
    DataException,
    UnpicklableArtifactException,
)
from metaflow.datastore.flow_datastore import FlowDataStore
from metaflow.exception import MetaflowException
from metaflow.plugins.datastores.local_storage import LocalStorage
from metaflow.plugins.datastores.serializers.pickle_serializer import PickleSerializer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_store():
    """
    Fixture to isolate SerializerStore global state per test.
    Prevents tests from poisoning the registry if they fail mid-execution.
    """
    saved_all = dict(SerializerStore._all_serializers)
    saved_active = set(SerializerStore._active_serializers)
    saved_records = dict(SerializerStore._records)
    saved_pending = dict(SerializerStore._pending_by_module)
    saved_cache = SerializerStore._ordered_cache

    yield

    SerializerStore._all_serializers.clear()
    SerializerStore._all_serializers.update(saved_all)
    SerializerStore._active_serializers.clear()
    SerializerStore._active_serializers.update(saved_active)
    SerializerStore._records.clear()
    SerializerStore._records.update(saved_records)
    SerializerStore._pending_by_module.clear()
    SerializerStore._pending_by_module.update(saved_pending)
    SerializerStore._ordered_cache = saved_cache


@pytest.fixture
def task_datastore(tmp_path):
    """Create a minimal TaskDataStore wired to a local storage backend."""
    storage_root = tmp_path / "datastore"
    storage_root.mkdir()

    flow_ds = FlowDataStore(
        flow_name="TestFlow",
        environment=None,
        metadata=None,
        event_logger=None,
        monitor=None,
        storage_impl=LocalStorage,
        ds_root=str(storage_root),
    )

    task_ds = flow_ds.get_task_datastore(
        run_id="1",
        step_name="start",
        task_id="1",
        attempt=0,
        mode="w",
    )
    task_ds.init_task()
    # Isolate from test serializers registered by other test files.
    # Only use PickleSerializer (as the plugin system would provide).
    task_ds._serializers = [PickleSerializer]
    return task_ds


# ---------------------------------------------------------------------------
# Test PickleSerializer round-trip through save/load artifacts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name, value",
    [
        ("my_dict", {"key": "value", "nested": [1, 2, 3]}),
        ("my_int", 42),
        ("my_str", "hello world"),
        ("my_none", None),
    ],
    ids=["dict", "int", "str", "none"],
)
def test_save_load_pickle_round_trip(task_datastore, name, value):
    """Standard Python objects go through PickleSerializer and round-trip."""
    task_datastore.save_artifacts(iter([(name, value)]))

    # Verify metadata
    info = task_datastore._info[name]
    assert "encoding" in info
    assert info["encoding"] == "pickle-v4"
    assert info["size"] > 0
    assert "type" in info

    # Load and verify
    loaded = dict(task_datastore.load_artifacts([name]))
    assert loaded[name] == value


def test_distinct_objects_on_load(task_datastore):
    """Loading the same artifact twice yields distinct object instances."""
    shared_list = [1, 2, 3]
    task_datastore.save_artifacts(iter([("a", shared_list), ("b", shared_list)]))

    loaded = dict(task_datastore.load_artifacts(["a", "b"]))
    assert loaded["a"] == loaded["b"]
    assert loaded["a"] is not loaded["b"]  # distinct instances


def test_metadata_auto_populates_source_for_pickle(task_datastore):
    """PickleSerializer returns empty serializer_info, but save_artifacts
    auto-injects ``source`` from the bootstrap-time record so load errors
    can tell the user which package provides the missing serializer."""
    task_datastore.save_artifacts(iter([("x", 42)]))
    info = task_datastore._info["x"]
    assert info.get("serializer_info", {}).get("source") == "metaflow"


def test_author_source_is_not_overridden(task_datastore, isolated_store):
    """A serializer that sets its own ``source`` in serializer_info should
    not have it overridden by the auto-injected bootstrap source."""

    class _ExplicitSourceSerializer(ArtifactSerializer):
        TYPE = "test_explicit_source"
        PRIORITY = 1

        @classmethod
        def can_serialize(cls, obj):
            return isinstance(obj, str)

        @classmethod
        def can_deserialize(cls, metadata):
            return metadata.encoding == "test_explicit_source"

        @classmethod
        def serialize(cls, obj, format=SerializationFormat.STORAGE):
            blob = obj.encode("utf-8")
            return (
                [SerializedBlob(blob, is_reference=False)],
                SerializationMetadata(
                    obj_type="str",
                    size=len(blob),
                    encoding="test_explicit_source",
                    serializer_info={"source": "i-picked-this-myself"},
                ),
            )

        @classmethod
        def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
            return data[0].decode("utf-8")

    # Seed a record so get_source_for would try to inject "some-extension"
    # — the author's explicit source should still win.
    rec = SerializerRecord(
        name="test_explicit_source",
        class_path="inline.ExplicitSourceSerializer",
        state=SerializerState.ACTIVE,
        type="test_explicit_source",
        source="some-extension",
    )
    SerializerStore._records["test_explicit_source"] = rec
    SerializerStore._active_serializers.add(_ExplicitSourceSerializer)

    task_datastore._serializers = [_ExplicitSourceSerializer, PickleSerializer]

    task_datastore.save_artifacts(iter([("hello", "world")]))
    info = task_datastore._info["hello"]
    assert info["serializer_info"]["source"] == "i-picked-this-myself"


# ---------------------------------------------------------------------------
# Test custom serializer takes priority
# ---------------------------------------------------------------------------


def test_custom_serializer_takes_priority(task_datastore, isolated_store):
    """A custom serializer with lower PRIORITY claims matching objects over pickle."""

    class _JsonStringSerializer(ArtifactSerializer):
        TYPE = "test_json_str"
        PRIORITY = 50

        @classmethod
        def can_serialize(cls, obj):
            return isinstance(obj, str)

        @classmethod
        def can_deserialize(cls, metadata):
            return metadata.encoding == "test_json_str"

        @classmethod
        def serialize(cls, obj, format=SerializationFormat.STORAGE):
            blob = json.dumps(obj).encode("utf-8")
            return (
                [SerializedBlob(blob, is_reference=False)],
                SerializationMetadata(
                    obj_type="str",
                    size=len(blob),
                    encoding="test_json_str",
                    serializer_info={"format": "json-utf8"},
                ),
            )

        @classmethod
        def deserialize(cls, data, metadata=None, format="storage"):
            return json.loads(data[0].decode("utf-8"))

    task_datastore._serializers = [_JsonStringSerializer, PickleSerializer]

    task_datastore.save_artifacts(iter([("msg", "hello"), ("num", 42)]))

    # "msg" should use our custom serializer (str → _JsonStringSerializer)
    msg_info = task_datastore._info["msg"]
    assert msg_info["encoding"] == "test_json_str"
    assert msg_info["serializer_info"] == {"format": "json-utf8"}

    # "num" should fall through to PickleSerializer (int → not claimed by custom)
    num_info = task_datastore._info["num"]
    assert num_info["encoding"] == "pickle-v4"

    # Both round-trip correctly
    loaded = dict(task_datastore.load_artifacts(["msg", "num"]))
    assert loaded["msg"] == "hello"
    assert loaded["num"] == 42


# ---------------------------------------------------------------------------
# Backward compat: old metadata format
# ---------------------------------------------------------------------------


def test_backward_compat_old_metadata(task_datastore):
    """Artifacts saved with old metadata format (no serializer_info) still load."""
    task_datastore.save_artifacts(iter([("old_artifact", {"a": 1})]))

    # Simulate old metadata format: no serializer_info, old encoding
    task_datastore._info["old_artifact"] = {
        "size": 100,
        "type": "<class 'dict'>",
        "encoding": "gzip+pickle-v4",
    }

    # Should still load via PickleSerializer
    loaded = dict(task_datastore.load_artifacts(["old_artifact"]))
    assert loaded["old_artifact"] == {"a": 1}


def test_backward_compat_no_encoding(task_datastore):
    """Very old artifacts without encoding field default to gzip+pickle-v2."""
    task_datastore.save_artifacts(iter([("ancient", 99)]))

    task_datastore._info["ancient"] = {
        "size": 10,
        "type": "<class 'int'>",
    }

    loaded = dict(task_datastore.load_artifacts(["ancient"]))
    assert loaded["ancient"] == 99


def test_missing_artifact_raises_key_error(task_datastore):
    """Missing artifacts preserve the historical KeyError contract."""
    task_datastore.save_artifacts(iter([("present", 1)]))

    with pytest.raises(KeyError):
        list(task_datastore.load_artifacts(["missing"]))


def test_missing_info_with_object_uses_pickle_defaults(task_datastore):
    """Artifacts without metadata still use very old pickle defaults."""
    task_datastore.save_artifacts(
        iter([("present", {"value": 1}), ("keeps_metadata_non_empty", 2)])
    )
    del task_datastore._info["present"]

    loaded = dict(task_datastore.load_artifacts(["present"]))
    assert loaded["present"] == {"value": 1}


def test_info_without_object_raises_key_error(task_datastore):
    """Metadata-only artifacts preserve the historical KeyError behavior."""
    task_datastore.save_artifacts(iter([("present", 1)]))
    task_datastore._info["metadata_only"] = {
        "size": 1,
        "type": "<class 'int'>",
        "encoding": "pickle-v4",
    }

    with pytest.raises(KeyError):
        list(task_datastore.load_artifacts(["metadata_only"]))


# ---------------------------------------------------------------------------
# Dynamic registry: lazy registrations reach long-lived datastores
# ---------------------------------------------------------------------------


def test_post_init_registration_reaches_existing_datastore(
    task_datastore, isolated_store
):
    """A serializer registered AFTER the datastore was constructed must still
    be visible. Without the dynamic ``_serializers`` property, lazy imports
    would be silently ignored for that instance."""
    task_datastore._serializers = None

    class _PostInitSerializer(ArtifactSerializer):
        TYPE = "test_post_init_registration"
        PRIORITY = 5

        @classmethod
        def can_serialize(cls, obj):
            return False

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format="storage"):
            raise NotImplementedError

        @classmethod
        def deserialize(cls, data, metadata=None, format="storage"):
            raise NotImplementedError

    SerializerStore._active_serializers.add(_PostInitSerializer)
    SerializerStore._ordered_cache = None

    assert _PostInitSerializer in task_datastore._serializers


# ---------------------------------------------------------------------------
# Blob-count validation must happen before ``_info`` is mutated
# ---------------------------------------------------------------------------


def test_info_not_populated_when_serializer_returns_no_blobs(
    task_datastore, isolated_store
):
    """If a serializer returns an empty blob list, ``save_artifacts`` must raise
    without leaving partial metadata in ``_info``."""

    class _EmptyBlobSerializer(ArtifactSerializer):
        TYPE = "test_empty_blob"
        PRIORITY = 5

        @classmethod
        def can_serialize(cls, obj):
            return True

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format="storage"):
            return ([], SerializationMetadata("x", 0, "test_empty_blob", {}))

        @classmethod
        def deserialize(cls, data, metadata=None, format="storage"):
            raise NotImplementedError

    task_datastore._serializers = [_EmptyBlobSerializer, PickleSerializer]

    with pytest.raises(DataException, match="returned no blobs"):
        task_datastore.save_artifacts(iter([("bad", object())]))
    assert "bad" not in task_datastore._info


def test_info_not_populated_when_serializer_returns_multi_blob(
    task_datastore, isolated_store
):
    """Same guarantee as above for the multi-blob rejection path."""

    class _MultiBlobSerializer(ArtifactSerializer):
        TYPE = "test_multi_blob"
        PRIORITY = 5

        @classmethod
        def can_serialize(cls, obj):
            return True

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format="storage"):
            return (
                [SerializedBlob(b"a"), SerializedBlob(b"b")],
                SerializationMetadata("x", 2, "test_multi_blob", {}),
            )

        @classmethod
        def deserialize(cls, data, metadata=None, format="storage"):
            raise NotImplementedError

    task_datastore._serializers = [_MultiBlobSerializer, PickleSerializer]

    with pytest.raises(DataException, match="single-blob serializers"):
        task_datastore.save_artifacts(iter([("bad", object())]))
    assert "bad" not in task_datastore._info


# ---------------------------------------------------------------------------
# Exception flow mapping
# ---------------------------------------------------------------------------


def test_pickle_serializer_raises_unpicklable_with_artifact_name(task_datastore):
    """PickleSerializer raises ``UnpicklableArtifactException``;
    ``save_artifacts`` re-raises it with the artifact name attached."""
    unpicklable = threading.Lock()

    with pytest.raises(UnpicklableArtifactException, match='named "bad_one"'):
        task_datastore.save_artifacts(iter([("bad_one", unpicklable)]))
    assert "bad_one" not in task_datastore._info


def test_extension_metaflow_exception_passes_through(task_datastore, isolated_store):
    """An extension serializer raising a ``MetaflowException`` subclass must
    propagate as-is."""

    class _ExtensionError(MetaflowException):
        headline = "Extension validation failed"

    class _RaisingSerializer(ArtifactSerializer):
        TYPE = "test_passthrough_ser"
        PRIORITY = 1

        @classmethod
        def can_serialize(cls, obj):
            return True

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format=SerializationFormat.STORAGE):
            raise _ExtensionError("schema mismatch on field 'foo'")

        @classmethod
        def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
            raise NotImplementedError

    task_datastore._serializers = [_RaisingSerializer, PickleSerializer]

    with pytest.raises(_ExtensionError, match="schema mismatch on field 'foo'"):
        task_datastore.save_artifacts(iter([("x", object())]))
    assert "x" not in task_datastore._info


def test_extension_type_error_is_not_mislabeled_unpicklable(
    task_datastore, isolated_store
):
    """A non-pickle serializer raising ``TypeError`` must NOT be reported as
    ``UnpicklableArtifactException``."""

    class _TypeErrorSerializer(ArtifactSerializer):
        TYPE = "test_typeerror_ser"
        PRIORITY = 1

        @classmethod
        def can_serialize(cls, obj):
            return True

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format=SerializationFormat.STORAGE):
            raise TypeError("custom serializer barfed on this type")

        @classmethod
        def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
            raise NotImplementedError

    task_datastore._serializers = [_TypeErrorSerializer, PickleSerializer]

    with pytest.raises(DataException, match="_TypeErrorSerializer") as exc_info:
        task_datastore.save_artifacts(iter([("x", object())]))

    assert not isinstance(exc_info.value, UnpicklableArtifactException)
    assert "x" not in task_datastore._info


def test_can_serialize_exception_falls_through_to_pickle(
    task_datastore, isolated_store
):
    """A buggy custom serializer's can_serialize exception must NOT crash
    save_artifacts. Pickle fallback handles it; dispatch_error_count is incremented."""

    class _BuggyCanSerialize(ArtifactSerializer):
        TYPE = "test_buggy_cs"
        PRIORITY = 1

        @classmethod
        def can_serialize(cls, obj):
            raise RuntimeError("intentional bug in can_serialize")

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format=SerializationFormat.STORAGE):
            raise NotImplementedError

        @classmethod
        def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
            raise NotImplementedError

    rec = SerializerRecord(
        name="test_buggy_cs",
        class_path="test.inline.BuggyCanSerialize",
        state=SerializerState.ACTIVE,
        type="test_buggy_cs",
        priority=1,
    )
    SerializerStore._records["test_buggy_cs"] = rec
    SerializerStore._active_serializers.add(_BuggyCanSerialize)

    task_datastore._serializers = [_BuggyCanSerialize, PickleSerializer]

    # Must NOT raise.
    task_datastore.save_artifacts(iter([("x", 42)]))
    assert task_datastore._info["x"]["encoding"] == "pickle-v4"
    assert rec.dispatch_error_count == 1
    assert "RuntimeError" in rec.last_error


def test_can_deserialize_exception_falls_through(task_datastore, isolated_store):
    """Same guarantee for can_deserialize during load_artifacts."""

    class _BuggyCanDeserialize(ArtifactSerializer):
        TYPE = "test_buggy_cd"
        PRIORITY = 1

        @classmethod
        def can_serialize(cls, obj):
            return False

        @classmethod
        def can_deserialize(cls, metadata):
            raise RuntimeError("intentional bug in can_deserialize")

        @classmethod
        def serialize(cls, obj, format=SerializationFormat.STORAGE):
            raise NotImplementedError

        @classmethod
        def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
            raise NotImplementedError

    rec = SerializerRecord(
        name="test_buggy_cd",
        class_path="test.inline.BuggyCanDeserialize",
        state=SerializerState.ACTIVE,
        type="test_buggy_cd",
        priority=1,
    )
    SerializerStore._records["test_buggy_cd"] = rec
    SerializerStore._active_serializers.add(_BuggyCanDeserialize)

    # First save an artifact normally via pickle
    task_datastore._serializers = [PickleSerializer]
    task_datastore.save_artifacts(iter([("y", "hello")]))

    # Now install the buggy serializer and try to load
    task_datastore._serializers = [_BuggyCanDeserialize, PickleSerializer]

    loaded = dict(task_datastore.load_artifacts(["y"]))
    assert loaded["y"] == "hello"
    assert rec.dispatch_error_count == 1
    assert "RuntimeError" in rec.last_error


def test_subclass_lazy_import_stashes_on_child_not_parent(isolated_store):
    """lazy_import on a subclass should set attrs on the subclass, not the parent.
    Parent and children should each have their own _lazy_imported_names set."""

    class _ParentSer(ArtifactSerializer):
        TYPE = "test_inherit_parent"

        @classmethod
        def setup_imports(cls, context=None):
            cls.lazy_import("json")

        @classmethod
        def can_serialize(cls, obj):
            return False

        @classmethod
        def can_deserialize(cls, metadata):
            return False

        @classmethod
        def serialize(cls, obj, format=SerializationFormat.STORAGE):
            raise NotImplementedError

        @classmethod
        def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
            raise NotImplementedError

    class _ChildSer(_ParentSer):
        TYPE = "test_inherit_child"

        @classmethod
        def setup_imports(cls, context=None):
            cls.lazy_import("sys")

    _ParentSer.setup_imports()
    _ChildSer.setup_imports()

    import json as _json
    import sys as _sys

    assert _ParentSer.json is _json
    assert _ChildSer.sys is _sys

    parent_names = _ParentSer.__dict__.get("_lazy_imported_names", set())
    child_names = _ChildSer.__dict__.get("_lazy_imported_names", set())
    assert parent_names == {"json"}
    assert child_names == {"sys"}
