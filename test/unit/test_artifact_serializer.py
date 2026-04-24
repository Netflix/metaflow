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
_PRE_IMPORT_ACTIVE_SNAPSHOT = set(SerializerStore._active_serializers)


@pytest.fixture(scope="module", autouse=True)
def _restore_serializer_registry():
    yield
    SerializerStore._all_serializers.clear()
    SerializerStore._all_serializers.update(_PRE_IMPORT_SNAPSHOT)
    SerializerStore._active_serializers.clear()
    SerializerStore._active_serializers.update(_PRE_IMPORT_ACTIVE_SNAPSHOT)
    SerializerStore._ordered_cache = None


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


# Dispatch is now driven by _active_serializers (post-Phase-6). The metaclass
# only populates _all_serializers; tests that assert against the ordered
# dispatch list must also mark their classes as active.
SerializerStore._active_serializers.add(_HighPrioritySerializer)
SerializerStore._active_serializers.add(_LowPrioritySerializer)
SerializerStore._active_serializers.add(_SamePrioritySerializer)
SerializerStore._ordered_cache = None


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


def test_priority_tie_last_wins():
    """When PRIORITY is equal, last-registered wins the tie."""

    class _TieFirst(ArtifactSerializer):
        TYPE = "test_tie_first"
        PRIORITY = 123

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

    class _TieSecond(ArtifactSerializer):
        TYPE = "test_tie_second"
        PRIORITY = 123  # same as _TieFirst

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

    try:
        SerializerStore._active_serializers.add(_TieFirst)
        SerializerStore._active_serializers.add(_TieSecond)
        SerializerStore._ordered_cache = None
        ordered = SerializerStore.get_ordered_serializers()
        idx_first = ordered.index(_TieFirst)
        idx_second = ordered.index(_TieSecond)
        # _TieSecond was registered LAST, so it should appear BEFORE _TieFirst.
        assert idx_second < idx_first, (
            "Expected last-registered (_TieSecond) to come first; got "
            "_TieFirst at index %d, _TieSecond at index %d" % (idx_first, idx_second)
        )
    finally:
        SerializerStore._all_serializers.pop("test_tie_first", None)
        SerializerStore._all_serializers.pop("test_tie_second", None)
        SerializerStore._active_serializers.discard(_TieFirst)
        SerializerStore._active_serializers.discard(_TieSecond)
        SerializerStore._ordered_cache = None


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


def test_priority_tie_lexicographic_fallback():
    """When PRIORITY and registration index both tie (simulated), class_path lex-sort wins."""

    # Within a single process, registration indices are always unique, so
    # to actually exercise the tertiary key we construct two classes with
    # identical (PRIORITY, registration_index) by manipulating the combined
    # list passed to the sort logic. We do this by calling the internal
    # sort key directly.
    class _AClass:
        __module__ = "z.module"
        __qualname__ = "AClass"
        PRIORITY = 100

    class _BClass:
        __module__ = "a.module"
        __qualname__ = "BClass"
        PRIORITY = 100

    # Same registration index (simulated): the (priority, -idx) prefix ties.
    keys = [
        (_AClass.PRIORITY, 0, "%s.%s" % (_AClass.__module__, _AClass.__qualname__)),
        (_BClass.PRIORITY, 0, "%s.%s" % (_BClass.__module__, _BClass.__qualname__)),
    ]
    sorted_keys = sorted(keys)
    # "a.module.BClass" < "z.module.AClass" lexicographically
    assert sorted_keys[0][2] == "a.module.BClass"
    assert sorted_keys[1][2] == "z.module.AClass"


def test_setup_imports_default_is_noop():
    """Default setup_imports should be callable and do nothing."""

    class _NoOverride(ArtifactSerializer):
        TYPE = "test_no_override"

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

    try:
        result = _NoOverride.setup_imports()
        assert result is None
        result = _NoOverride.setup_imports(context="anything")
        assert result is None
    finally:
        SerializerStore._all_serializers.pop("test_no_override", None)
        SerializerStore._ordered_cache = None


def test_lazy_import_happy_path():
    """lazy_import imports the module, stashes on cls at the leaf alias, and returns it."""

    class _LazyOk(ArtifactSerializer):
        TYPE = "test_lazy_ok"

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

    try:
        mod = _LazyOk.lazy_import("json")
        import json as _json

        assert mod is _json
        assert _LazyOk.json is _json
    finally:
        SerializerStore._all_serializers.pop("test_lazy_ok", None)
        SerializerStore._ordered_cache = None
        if hasattr(_LazyOk, "json"):
            delattr(_LazyOk, "json")


def test_lazy_import_custom_alias():
    """alias= overrides the default leaf-name stash key."""

    class _LazyAlias(ArtifactSerializer):
        TYPE = "test_lazy_alias"

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

    try:
        _LazyAlias.lazy_import("json", alias="j")
        import json as _json

        assert _LazyAlias.j is _json
    finally:
        SerializerStore._all_serializers.pop("test_lazy_alias", None)
        SerializerStore._ordered_cache = None
        if hasattr(_LazyAlias, "j"):
            delattr(_LazyAlias, "j")


def test_lazy_import_rejects_reserved_names():
    """Attempting to shadow TYPE / PRIORITY / dispatch methods raises."""

    class _LazyReserved(ArtifactSerializer):
        TYPE = "test_lazy_reserved"

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

    try:
        for bad in [
            "TYPE",
            "PRIORITY",
            "serialize",
            "deserialize",
            "can_serialize",
            "can_deserialize",
            "setup_imports",
            "lazy_import",
            "_secret",
        ]:
            with pytest.raises(ValueError, match="reserved or invalid"):
                _LazyReserved.lazy_import("json", alias=bad)
    finally:
        SerializerStore._all_serializers.pop("test_lazy_reserved", None)
        SerializerStore._ordered_cache = None


def test_lazy_import_rejects_double_assignment():
    """Calling lazy_import twice with the same alias on the same cls raises."""

    class _LazyDup(ArtifactSerializer):
        TYPE = "test_lazy_dup"

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

    try:
        _LazyDup.lazy_import("json")
        with pytest.raises(ValueError, match="already set"):
            _LazyDup.lazy_import("sys", alias="json")
    finally:
        SerializerStore._all_serializers.pop("test_lazy_dup", None)
        SerializerStore._ordered_cache = None
        if hasattr(_LazyDup, "json"):
            delattr(_LazyDup, "json")
        if hasattr(_LazyDup, "_lazy_imported_names"):
            delattr(_LazyDup, "_lazy_imported_names")


def test_setup_imports_accepts_both_signatures():
    """Bootstrap calls setup_imports correctly whether author writes (cls) or (cls, context=None)."""

    class _OneArg(ArtifactSerializer):
        TYPE = "test_setup_one_arg"
        called = False

        @classmethod
        def setup_imports(cls):
            cls.called = True

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

    class _TwoArg(ArtifactSerializer):
        TYPE = "test_setup_two_arg"
        called_with = "sentinel"

        @classmethod
        def setup_imports(cls, context=None):
            cls.called_with = context

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

    from metaflow.datastore.artifacts.serializer import _call_setup_imports

    try:
        _call_setup_imports(_OneArg, context=None)
        assert _OneArg.called is True

        _call_setup_imports(_TwoArg, context="some-ctx")
        assert _TwoArg.called_with == "some-ctx"
    finally:
        SerializerStore._all_serializers.pop("test_setup_one_arg", None)
        SerializerStore._all_serializers.pop("test_setup_two_arg", None)
        SerializerStore._ordered_cache = None
