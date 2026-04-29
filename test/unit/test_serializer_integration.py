"""
Integration tests for the pluggable serializer framework wired into TaskDataStore.

Tests that:
- PickleSerializer handles standard Python objects through save/load_artifacts
- Custom serializers take priority over PickleSerializer
- Backward compat: old artifacts (without serializer_info) still load
- Metadata includes serializer_info when present
"""

import json
import os
import shutil
import tempfile

import pytest

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationFormat,
    SerializationMetadata,
    SerializedBlob,
    SerializerStore,
)
from metaflow.plugins.datastores.serializers.pickle_serializer import PickleSerializer


# ---------------------------------------------------------------------------
# Test PickleSerializer round-trip through save/load artifacts
# ---------------------------------------------------------------------------


@pytest.fixture
def task_datastore(tmp_path):
    """Create a minimal TaskDataStore wired to a local storage backend."""
    from metaflow.datastore.flow_datastore import FlowDataStore
    from metaflow.plugins.datastores.local_storage import LocalStorage

    storage_root = str(tmp_path / "datastore")
    os.makedirs(storage_root, exist_ok=True)

    flow_ds = FlowDataStore(
        flow_name="TestFlow",
        environment=None,
        metadata=None,
        event_logger=None,
        monitor=None,
        storage_impl=LocalStorage,
        ds_root=storage_root,
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


def test_save_load_pickle_round_trip(task_datastore):
    """Standard Python objects go through PickleSerializer and round-trip."""
    artifacts = [
        ("my_dict", {"key": "value", "nested": [1, 2, 3]}),
        ("my_int", 42),
        ("my_str", "hello world"),
        ("my_none", None),
    ]
    task_datastore.save_artifacts(iter(artifacts))

    # Verify metadata
    for name, _ in artifacts:
        info = task_datastore._info[name]
        assert "encoding" in info
        assert info["encoding"] == "pickle-v4"
        assert info["size"] > 0
        assert "type" in info

    # Load and verify
    loaded = dict(task_datastore.load_artifacts([name for name, _ in artifacts]))
    assert loaded["my_dict"] == {"key": "value", "nested": [1, 2, 3]}
    assert loaded["my_int"] == 42
    assert loaded["my_str"] == "hello world"
    assert loaded["my_none"] is None


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


def test_author_source_is_not_overridden(task_datastore):
    """A serializer that sets its own ``source`` in serializer_info should
    not have it overridden by the auto-injected bootstrap source."""
    from metaflow.datastore.artifacts import SerializationFormat, SerializerStore
    from metaflow.datastore.artifacts.diagnostic import (
        SerializerRecord,
        SerializerState,
    )

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

    try:
        task_datastore.save_artifacts(iter([("hello", "world")]))
        info = task_datastore._info["hello"]
        assert info["serializer_info"]["source"] == "i-picked-this-myself"
    finally:
        SerializerStore._records.pop("test_explicit_source", None)
        SerializerStore._active_serializers.discard(_ExplicitSourceSerializer)
        SerializerStore._all_serializers.pop("test_explicit_source", None)
        SerializerStore._ordered_cache = None


# ---------------------------------------------------------------------------
# Test custom serializer takes priority
# ---------------------------------------------------------------------------


def test_custom_serializer_takes_priority(task_datastore):
    """A custom serializer with lower PRIORITY claims matching objects over pickle."""

    # Define and register a custom serializer inside the test
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

    # Explicitly set serializers: custom first, then pickle fallback.
    # Don't use get_ordered_serializers() to avoid pollution from other test files.
    task_datastore._serializers = [_JsonStringSerializer, PickleSerializer]

    try:
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
    finally:
        SerializerStore._all_serializers.pop("test_json_str", None)
        SerializerStore._ordered_cache = None


# ---------------------------------------------------------------------------
# Backward compat: old metadata format
# ---------------------------------------------------------------------------


def test_backward_compat_old_metadata(task_datastore):
    """Artifacts saved with old metadata format (no serializer_info) still load."""
    # Save normally first
    task_datastore.save_artifacts(iter([("old_artifact", {"a": 1})]))

    # Simulate old metadata format: no serializer_info, old encoding
    task_datastore._info["old_artifact"] = {
        "size": 100,
        "type": "<class 'dict'>",
        "encoding": "gzip+pickle-v4",
        # no "serializer_info" key
    }

    # Should still load via PickleSerializer (can_deserialize handles gzip+pickle-v4)
    loaded = dict(task_datastore.load_artifacts(["old_artifact"]))
    assert loaded["old_artifact"] == {"a": 1}


def test_backward_compat_no_encoding(task_datastore):
    """Very old artifacts without encoding field default to gzip+pickle-v2."""
    # Save an artifact
    task_datastore.save_artifacts(iter([("ancient", 99)]))

    # Simulate very old metadata: no encoding, no serializer_info
    task_datastore._info["ancient"] = {
        "size": 10,
        "type": "<class 'int'>",
        # no "encoding" key — defaults to gzip+pickle-v2
    }

    # Should still load
    loaded = dict(task_datastore.load_artifacts(["ancient"]))
    assert loaded["ancient"] == 99


# ---------------------------------------------------------------------------
# Dynamic registry: lazy registrations reach long-lived datastores
# ---------------------------------------------------------------------------


def test_post_init_registration_reaches_existing_datastore(task_datastore):
    """A serializer registered AFTER the datastore was constructed must still
    be visible. Without the dynamic ``_serializers`` property, lazy imports
    (e.g. ``import torch`` after ``TaskDataStore.__init__``) would be silently
    ignored for that instance.
    """
    # Drop the test override so the property falls back to the live registry.
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

    # Dispatch reads from _active_serializers now (post-Phase-6).
    SerializerStore._active_serializers.add(_PostInitSerializer)
    SerializerStore._ordered_cache = None
    try:
        assert _PostInitSerializer in task_datastore._serializers
    finally:
        SerializerStore._all_serializers.pop("test_post_init_registration", None)
        SerializerStore._active_serializers.discard(_PostInitSerializer)
        SerializerStore._ordered_cache = None


# ---------------------------------------------------------------------------
# Blob-count validation must happen before ``_info`` is mutated
# ---------------------------------------------------------------------------


def test_info_not_populated_when_serializer_returns_no_blobs(task_datastore):
    """
    Regression for the "_info[name] poisoned on validation failure" bug: if a
    serializer returns an empty blob list, ``save_artifacts`` must raise
    without leaving partial metadata in ``_info``.
    """
    from metaflow.datastore.exceptions import DataException

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
            return (
                [],
                SerializationMetadata("x", 0, "test_empty_blob", {}),
            )

        @classmethod
        def deserialize(cls, data, metadata=None, format="storage"):
            raise NotImplementedError

    task_datastore._serializers = [_EmptyBlobSerializer, PickleSerializer]
    try:
        with pytest.raises(DataException, match="returned no blobs"):
            task_datastore.save_artifacts(iter([("bad", object())]))
        assert "bad" not in task_datastore._info
    finally:
        SerializerStore._all_serializers.pop("test_empty_blob", None)
        SerializerStore._ordered_cache = None


def test_info_not_populated_when_serializer_returns_multi_blob(task_datastore):
    """Same guarantee as above for the multi-blob rejection path."""
    from metaflow.datastore.exceptions import DataException

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
    try:
        with pytest.raises(DataException, match="single-blob serializers"):
            task_datastore.save_artifacts(iter([("bad", object())]))
        assert "bad" not in task_datastore._info
    finally:
        SerializerStore._all_serializers.pop("test_multi_blob", None)
        SerializerStore._ordered_cache = None


def test_can_serialize_exception_falls_through_to_pickle(task_datastore):
    """A buggy custom serializer's can_serialize exception must NOT crash
    save_artifacts. The buggy serializer is skipped; pickle fallback handles
    the artifact; dispatch_error_count is incremented."""
    from metaflow.datastore.artifacts import SerializationFormat
    from metaflow.datastore.artifacts.diagnostic import (
        SerializerRecord,
        SerializerState,
    )

    class _BuggyCanSerialize(ArtifactSerializer):
        TYPE = "test_buggy_cs"
        PRIORITY = 1  # tried first

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

    # Seed a diagnostic record so dispatch_error_count has somewhere to go.
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

    try:
        # Must NOT raise.
        task_datastore.save_artifacts(iter([("x", 42)]))
        assert task_datastore._info["x"]["encoding"] == "pickle-v4"
        assert rec.dispatch_error_count == 1
        assert rec.last_error is not None
        assert "RuntimeError" in rec.last_error
    finally:
        SerializerStore._all_serializers.pop("test_buggy_cs", None)
        SerializerStore._active_serializers.discard(_BuggyCanSerialize)
        SerializerStore._records.pop("test_buggy_cs", None)
        SerializerStore._ordered_cache = None


def test_can_deserialize_exception_falls_through(task_datastore):
    """Same guarantee for can_deserialize during load_artifacts."""
    from metaflow.datastore.artifacts import SerializationFormat
    from metaflow.datastore.artifacts.diagnostic import (
        SerializerRecord,
        SerializerState,
    )

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

    # First save an artifact normally via pickle so load has something to load.
    task_datastore._serializers = [PickleSerializer]
    task_datastore.save_artifacts(iter([("y", "hello")]))

    # Now install the buggy serializer and try to load — buggy can_deserialize
    # should be skipped and pickle should take over.
    task_datastore._serializers = [_BuggyCanDeserialize, PickleSerializer]

    try:
        loaded = dict(task_datastore.load_artifacts(["y"]))
        assert loaded["y"] == "hello"
        assert rec.dispatch_error_count == 1
        assert rec.last_error is not None
        assert "RuntimeError" in rec.last_error
    finally:
        SerializerStore._all_serializers.pop("test_buggy_cd", None)
        SerializerStore._active_serializers.discard(_BuggyCanDeserialize)
        SerializerStore._records.pop("test_buggy_cd", None)
        SerializerStore._ordered_cache = None


def test_subclass_lazy_import_stashes_on_child_not_parent():
    """lazy_import on a subclass should set attrs on the subclass, not the parent.
    Parent and children should each have their own _lazy_imported_names set."""
    from metaflow.datastore.artifacts import (
        ArtifactSerializer,
        SerializationFormat,
        SerializerStore,
    )

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

    try:
        _ParentSer.setup_imports()
        _ChildSer.setup_imports()
        import json as _json
        import sys as _sys

        # Parent has json; child has sys
        assert _ParentSer.json is _json
        assert _ChildSer.sys is _sys

        # Each class should have its OWN _lazy_imported_names set
        # (not a shared inherited one)
        parent_names = _ParentSer.__dict__.get("_lazy_imported_names", set())
        child_names = _ChildSer.__dict__.get("_lazy_imported_names", set())
        assert parent_names == {"json"}
        assert child_names == {"sys"}
    finally:
        for t in ("test_inherit_parent", "test_inherit_child"):
            SerializerStore._all_serializers.pop(t, None)
        SerializerStore._ordered_cache = None
        for c, attr in ((_ParentSer, "json"), (_ChildSer, "sys")):
            if attr in c.__dict__:
                delattr(c, attr)
        for c in (_ParentSer, _ChildSer):
            if "_lazy_imported_names" in c.__dict__:
                delattr(c, "_lazy_imported_names")
