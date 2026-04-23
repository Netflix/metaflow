import pytest

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationFormat,
    SerializationMetadata,
    SerializedBlob,
    SerializerStore,
)


# Snapshot the registry before this module's classes are defined. Module-level
# test serializers (_HighPrioritySerializer, ...) self-register at class
# definition time; the module-scoped fixture below removes them at teardown so
# other test modules see an unpolluted registry.
_PRE_IMPORT_SNAPSHOT = dict(SerializerStore._all_serializers)


@pytest.fixture(scope="module", autouse=True)
def _restore_serializer_registry():
    yield
    SerializerStore._all_serializers.clear()
    SerializerStore._all_serializers.update(_PRE_IMPORT_SNAPSHOT)
    SerializerStore._ordered_cache = None
    SerializerStore._materialized_lazy_classes.clear()
    SerializerStore._materialized_lazy_types.clear()


# ---------------------------------------------------------------------------
# Helpers — test serializer subclasses defined inside the test module
# ---------------------------------------------------------------------------


class _HighPrioritySerializer(ArtifactSerializer):
    TYPE = "test_high"
    PRIORITY = 10

    @classmethod
    def can_serialize(cls, obj):
        return isinstance(obj, str)

    @classmethod
    def can_deserialize(cls, metadata):
        return metadata.encoding == "test_high"

    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        blob = obj.encode("utf-8")
        return (
            [SerializedBlob(blob)],
            SerializationMetadata("str", len(blob), "test_high", {}),
        )

    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        return data[0].decode("utf-8")


class _LowPrioritySerializer(ArtifactSerializer):
    TYPE = "test_low"
    PRIORITY = 200

    @classmethod
    def can_serialize(cls, obj):
        return isinstance(obj, int)

    @classmethod
    def can_deserialize(cls, metadata):
        return metadata.encoding == "test_low"

    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        blob = str(obj).encode("utf-8")
        return (
            [SerializedBlob(blob)],
            SerializationMetadata("int", len(blob), "test_low", {}),
        )

    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        return int(data[0].decode("utf-8"))


class _SamePrioritySerializer(ArtifactSerializer):
    """Same PRIORITY as default (100), registered after _HighPriority and _LowPriority."""

    TYPE = "test_default_priority"
    PRIORITY = 100

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


# ---------------------------------------------------------------------------
# SerializerStore tests
# ---------------------------------------------------------------------------


def test_auto_registration():
    """Subclasses with non-None TYPE are auto-registered."""
    assert "test_high" in SerializerStore._all_serializers
    assert "test_low" in SerializerStore._all_serializers
    assert SerializerStore._all_serializers["test_high"] is _HighPrioritySerializer
    assert SerializerStore._all_serializers["test_low"] is _LowPrioritySerializer


def test_base_class_not_registered():
    """ArtifactSerializer itself (TYPE=None) is not registered."""
    assert None not in SerializerStore._all_serializers


def test_re_registration_overwrites():
    """A second class with the same TYPE overwrites the first (notebook-friendly)."""
    original = SerializerStore._all_serializers["test_high"]
    try:

        class _ReplacementSerializer(ArtifactSerializer):
            TYPE = "test_high"  # same as _HighPrioritySerializer
            PRIORITY = 1

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
            def deserialize(
                cls, data, metadata=None, format=SerializationFormat.STORAGE
            ):
                raise NotImplementedError

        assert SerializerStore._all_serializers["test_high"] is _ReplacementSerializer
    finally:
        SerializerStore._all_serializers["test_high"] = original
        SerializerStore._ordered_cache = None


def test_priority_ordering():
    """get_ordered_serializers returns lower PRIORITY first."""
    ordered = SerializerStore.get_ordered_serializers()
    priorities = [s.PRIORITY for s in ordered]
    assert priorities == sorted(priorities)


def test_registration_order_tiebreaker():
    """When PRIORITY is equal, registration order breaks the tie."""
    ordered = SerializerStore.get_ordered_serializers()
    priority_100 = [s for s in ordered if s.PRIORITY == 100]
    if len(priority_100) > 1:
        registration_order = list(SerializerStore._all_serializers)
        indices = [registration_order.index(s.TYPE) for s in priority_100]
        assert indices == sorted(indices)


def test_deterministic_ordering():
    """Calling get_ordered_serializers twice returns the same order."""
    first = SerializerStore.get_ordered_serializers()
    second = SerializerStore.get_ordered_serializers()
    assert [s.TYPE for s in first] == [s.TYPE for s in second]


def test_high_priority_before_low():
    """_HighPrioritySerializer (PRIORITY=10) comes before _LowPrioritySerializer (PRIORITY=200)."""
    ordered = SerializerStore.get_ordered_serializers()
    types = [s.TYPE for s in ordered]
    assert types.index("test_high") < types.index("test_low")


def test_ordered_cache_not_rebuilt_in_steady_state(monkeypatch):
    """
    Regression for the "cache rebuilt on every call once a lazy config is
    registered" bug: when no new lazy classes become importable between
    successive calls, ``get_ordered_serializers`` must return the same cached
    list object — not a freshly-sorted one.
    """
    import sys as _sys
    import types as _types

    from metaflow.datastore.artifacts.lazy_registry import (
        SerializerConfig,
        register_serializer_config,
    )

    class _LazyProbeCacheSerializer(ArtifactSerializer):
        TYPE = "test_lazy_probe_cache"
        PRIORITY = 500

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

    fake = _types.ModuleType("_lazy_probe_cache_mod")
    fake._LazyProbeCacheSerializer = _LazyProbeCacheSerializer
    monkeypatch.setitem(_sys.modules, "_lazy_probe_cache_mod", fake)

    # Remove the self-registration so the lazy registry is the only path that
    # can put this class into the dispatch list.
    SerializerStore._all_serializers.pop("test_lazy_probe_cache", None)
    SerializerStore._ordered_cache = None

    try:
        register_serializer_config(
            SerializerConfig(
                canonical_type="_lazy_probe_cache_mod._LazyProbeCacheSerializer",
                serializer="_lazy_probe_cache_mod._LazyProbeCacheSerializer",
            )
        )

        # First call materializes the lazy class and caches the ordered list.
        first = SerializerStore.get_ordered_serializers()
        assert _LazyProbeCacheSerializer in first

        # Subsequent calls with no new registrations must return the *same*
        # cached list object — this is the assertion the previous
        # implementation failed.
        second = SerializerStore.get_ordered_serializers()
        third = SerializerStore.get_ordered_serializers()
        assert first is second
        assert second is third
    finally:
        SerializerStore._all_serializers.pop("test_lazy_probe_cache", None)
        SerializerStore._materialized_lazy_classes[:] = [
            c
            for c in SerializerStore._materialized_lazy_classes
            if getattr(c, "TYPE", None) != "test_lazy_probe_cache"
        ]
        SerializerStore._materialized_lazy_types.discard(
            "_lazy_probe_cache_mod._LazyProbeCacheSerializer"
        )
        SerializerStore._ordered_cache = None


# ---------------------------------------------------------------------------
# SerializationMetadata tests
# ---------------------------------------------------------------------------


def test_metadata_fields():
    meta = SerializationMetadata(
        obj_type="dict",
        size=1024,
        encoding="pickle-v4",
        serializer_info={"key": "value"},
    )
    assert meta.obj_type == "dict"
    assert meta.size == 1024
    assert meta.encoding == "pickle-v4"
    assert meta.serializer_info == {"key": "value"}


def test_metadata_is_namedtuple():
    meta = SerializationMetadata("str", 10, "utf-8", {})
    assert isinstance(meta, tuple)
    assert len(meta) == 4


# ---------------------------------------------------------------------------
# SerializedBlob tests
# ---------------------------------------------------------------------------


def test_blob_bytes_auto_detect():
    """bytes value auto-detects as not a reference."""
    blob = SerializedBlob(b"hello")
    assert blob.is_reference is False
    assert blob.needs_save is True


def test_blob_str_auto_detect():
    """str value auto-detects as a reference."""
    blob = SerializedBlob("sha1_key_abc123")
    assert blob.is_reference is True
    assert blob.needs_save is False


def test_blob_explicit_is_reference_override():
    """Explicit is_reference overrides auto-detection."""
    # bytes but marked as reference (edge case)
    blob = SerializedBlob(b"data", is_reference=True)
    assert blob.is_reference is True
    assert blob.needs_save is False

    # str but marked as not a reference (edge case)
    blob = SerializedBlob("inline_data", is_reference=False)
    assert blob.is_reference is False
    assert blob.needs_save is True


def test_blob_value_preserved():
    data = b"\x00\x01\x02\x03"
    blob = SerializedBlob(data)
    assert blob.value is data

    key = "abc123def456"
    blob = SerializedBlob(key)
    assert blob.value is key


def test_blob_rejects_invalid_types():
    """SerializedBlob must be str or bytes — reject everything else."""
    for bad_value in [123, 3.14, None, [], {}]:
        with pytest.raises(TypeError, match="must be str or bytes"):
            SerializedBlob(bad_value)


# ---------------------------------------------------------------------------
# Wire vs storage format dispatch
# ---------------------------------------------------------------------------


class _DualFormatSerializer(ArtifactSerializer):
    """Toy serializer that implements both formats for str objects."""

    TYPE = "test_dual_format"
    PRIORITY = 40

    @classmethod
    def can_serialize(cls, obj):
        return isinstance(obj, str)

    @classmethod
    def can_deserialize(cls, metadata):
        return metadata.encoding == "test_dual_format"

    @classmethod
    def serialize(cls, obj, format=SerializationFormat.STORAGE):
        if format == SerializationFormat.WIRE:
            return obj
        blob = obj.encode("utf-8")
        return (
            [SerializedBlob(blob)],
            SerializationMetadata("str", len(blob), "test_dual_format", {}),
        )

    @classmethod
    def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
        if format == SerializationFormat.WIRE:
            return data
        return data[0].decode("utf-8")


def test_format_enum_values():
    assert SerializationFormat.STORAGE.value == "storage"
    assert SerializationFormat.WIRE.value == "wire"
    # str-backed Enum, so direct string comparison still works.
    assert SerializationFormat.STORAGE == "storage"
    assert SerializationFormat.WIRE == "wire"


def test_dual_format_storage_roundtrip():
    blobs, meta = _DualFormatSerializer.serialize("hello")
    assert meta.encoding == "test_dual_format"
    assert (
        _DualFormatSerializer.deserialize([b.value for b in blobs], metadata=meta)
        == "hello"
    )


def test_dual_format_wire_roundtrip():
    wire = _DualFormatSerializer.serialize("hello", format=SerializationFormat.WIRE)
    assert isinstance(wire, str)
    assert (
        _DualFormatSerializer.deserialize(wire, format=SerializationFormat.WIRE)
        == "hello"
    )


def test_pickle_serializer_rejects_wire():
    from metaflow.plugins.datastores.serializers.pickle_serializer import (
        PickleSerializer,
    )

    with pytest.raises(NotImplementedError):
        PickleSerializer.serialize(42, format=SerializationFormat.WIRE)
    with pytest.raises(NotImplementedError):
        PickleSerializer.deserialize("42", format=SerializationFormat.WIRE)
