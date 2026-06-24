import pytest

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationFormat,
    SerializationMetadata,
    SerializedBlob,
    SerializerStore,
)

# ---------------------------------------------------------------------------
# Registry Isolation Setup & Fixtures
# ---------------------------------------------------------------------------

# Snapshot the registry before this module's classes are defined. Module-level
# test serializers self-register at class definition time; the module-scoped
# fixture below removes them at teardown so other modules see an unpolluted registry.
_PRE_IMPORT_SNAPSHOT = dict(SerializerStore._all_serializers)
_PRE_IMPORT_ACTIVE_SNAPSHOT = set(SerializerStore._active_serializers)


@pytest.fixture(scope="module", autouse=True)
def _restore_module_serializer_registry():
    yield
    SerializerStore._all_serializers.clear()
    SerializerStore._all_serializers.update(_PRE_IMPORT_SNAPSHOT)
    SerializerStore._active_serializers.clear()
    SerializerStore._active_serializers.update(_PRE_IMPORT_ACTIVE_SNAPSHOT)
    SerializerStore._ordered_cache = None


@pytest.fixture
def clean_store():
    """Fixture to cleanly revert mutations to the SerializerStore registry per test."""
    all_snapshot = dict(SerializerStore._all_serializers)
    active_snapshot = set(SerializerStore._active_serializers)
    yield
    SerializerStore._all_serializers.clear()
    SerializerStore._all_serializers.update(all_snapshot)
    SerializerStore._active_serializers.clear()
    SerializerStore._active_serializers.update(active_snapshot)
    SerializerStore._ordered_cache = None


# ---------------------------------------------------------------------------
# Helpers — Shared Test Serializer Subclasses
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


# Dispatch is driven by _active_serializers. The metaclass populates _all_serializers;
# tests asserting against the ordered dispatch list must also mark these classes active.
SerializerStore._active_serializers.add(_HighPrioritySerializer)
SerializerStore._active_serializers.add(_LowPrioritySerializer)
SerializerStore._active_serializers.add(_SamePrioritySerializer)
SerializerStore._ordered_cache = None


# ---------------------------------------------------------------------------
# SerializerStore Tests
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


def test_re_registration_overwrites(clean_store):
    """A second class with the same TYPE overwrites the first (notebook-friendly)."""

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
        def deserialize(cls, data, metadata=None, format=SerializationFormat.STORAGE):
            raise NotImplementedError

    assert SerializerStore._all_serializers["test_high"] is _ReplacementSerializer


def test_priority_ordering():
    """get_ordered_serializers returns lower PRIORITY first."""
    ordered = SerializerStore.get_ordered_serializers()
    priorities = [s.PRIORITY for s in ordered]
    assert priorities == sorted(priorities)


def test_priority_tie_last_wins(clean_store):
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

    SerializerStore._active_serializers.add(_TieFirst)
    SerializerStore._active_serializers.add(_TieSecond)
    SerializerStore._ordered_cache = None

    ordered = SerializerStore.get_ordered_serializers()
    idx_first = ordered.index(_TieFirst)
    idx_second = ordered.index(_TieSecond)

    # _TieSecond was registered LAST, so it should appear BEFORE _TieFirst.
    assert idx_second < idx_first, (
        f"Expected last-registered (_TieSecond) to come first; got "
        f"_TieFirst at index {idx_first}, _TieSecond at index {idx_second}"
    )


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


def test_priority_tie_lexicographic_fallback():
    """When PRIORITY and registration index both tie (simulated), class_path lex-sort wins."""

    # Within a single process, registration indices are unique. To test the tertiary
    # key fallback, we manually simulate identical prefixes on the internal sort key.
    class _AClass:
        __module__ = "z.module"
        __qualname__ = "AClass"
        PRIORITY = 100

    class _BClass:
        __module__ = "a.module"
        __qualname__ = "BClass"
        PRIORITY = 100

    keys = [
        (_AClass.PRIORITY, 0, f"{_AClass.__module__}.{_AClass.__qualname__}"),
        (_BClass.PRIORITY, 0, f"{_BClass.__module__}.{_BClass.__qualname__}"),
    ]
    sorted_keys = sorted(keys)

    # "a.module.BClass" < "z.module.AClass" lexicographically
    assert sorted_keys[0][2] == "a.module.BClass"
    assert sorted_keys[1][2] == "z.module.AClass"


# ---------------------------------------------------------------------------
# SerializationMetadata Tests
# ---------------------------------------------------------------------------


def test_metadata_fields():
    """Verify attributes are assigned and mapped properly on metadata container."""
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
    """Verify that SerializationMetadata preserves namedtuple traits."""
    meta = SerializationMetadata("str", 10, "utf-8", {})
    assert isinstance(meta, tuple)
    assert len(meta) == 4


# ---------------------------------------------------------------------------
# SerializedBlob Tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, kwargs, expected_is_reference, expected_needs_save",
    [
        (b"hello", {}, False, True),
        ("sha1_key_abc123", {}, True, False),
        (b"data", {"is_reference": True}, True, False),
        ("inline_data", {"is_reference": False}, False, True),
    ],
    ids=[
        "bytes-auto-detect-payload",
        "str-auto-detect-reference",
        "bytes-explicit-reference-override",
        "str-explicit-payload-override",
    ],
)
def test_blob_reference_detection(
    value, kwargs, expected_is_reference, expected_needs_save
):
    """Verify SerializedBlob reference tracking and explicit override behaviors."""
    blob = SerializedBlob(value, **kwargs)
    assert blob.is_reference is expected_is_reference
    assert blob.needs_save is expected_needs_save


@pytest.mark.parametrize(
    "data",
    [b"\x00\x01\x02\x03", "abc123def456"],
    ids=["bytes-payload", "str-reference"],
)
def test_blob_value_preserved(data):
    """Verify values given to the SerializedBlob initialization are kept identical."""
    blob = SerializedBlob(data)
    assert blob.value is data


@pytest.mark.parametrize(
    "bad_value",
    [123, 3.14, None, [], {}],
    ids=["int", "float", "none", "list", "dict"],
)
def test_blob_rejects_invalid_types(bad_value):
    """SerializedBlob must be str or bytes — reject everything else."""
    with pytest.raises(TypeError, match="must be str or bytes"):
        SerializedBlob(bad_value)


# ---------------------------------------------------------------------------
# Wire vs Storage Format Dispatch
# ---------------------------------------------------------------------------


def test_format_enum_values():
    """Verify the string mapping properties of the SerializationFormat enum."""
    assert SerializationFormat.STORAGE.value == "storage"
    assert SerializationFormat.WIRE.value == "wire"
    assert SerializationFormat.STORAGE == "storage"
    assert SerializationFormat.WIRE == "wire"


def test_dual_format_storage_roundtrip():
    """Verify the basic storage format workflow serialization loop."""
    blobs, meta = _DualFormatSerializer.serialize("hello")
    assert meta.encoding == "test_dual_format"
    assert (
        _DualFormatSerializer.deserialize([b.value for b in blobs], metadata=meta)
        == "hello"
    )


def test_dual_format_wire_roundtrip():
    """Verify the wire format workflow serialization loop."""
    wire = _DualFormatSerializer.serialize("hello", format=SerializationFormat.WIRE)
    assert isinstance(wire, str)
    assert (
        _DualFormatSerializer.deserialize(wire, format=SerializationFormat.WIRE)
        == "hello"
    )


def test_pickle_serializer_rejects_wire():
    """Verify standard PickleSerializer errors explicitly when utilizing the wire format."""
    from metaflow.plugins.datastores.serializers.pickle_serializer import (
        PickleSerializer,
    )

    with pytest.raises(NotImplementedError):
        PickleSerializer.serialize(42, format=SerializationFormat.WIRE)
    with pytest.raises(NotImplementedError):
        PickleSerializer.deserialize("42", format=SerializationFormat.WIRE)


# ---------------------------------------------------------------------------
# Setup and Lazy Import Infrastructure Tests
# ---------------------------------------------------------------------------


def test_setup_imports_default_is_noop(clean_store):
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

    assert _NoOverride.setup_imports() is None
    assert _NoOverride.setup_imports(context="anything") is None


@pytest.mark.parametrize(
    "module_name, alias, target_attr",
    [
        ("json", None, "json"),
        ("json", "j", "j"),
    ],
    ids=["default-leaf-alias", "custom-alias"],
)
def test_lazy_import_success(clean_store, module_name, alias, target_attr):
    """lazy_import imports the module, stashes on cls at the given alias, and returns it."""

    class _LazyTarget(ArtifactSerializer):
        TYPE = "test_lazy_target"

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

    import json as _json

    kwargs = {"alias": alias} if alias else {}
    mod = _LazyTarget.lazy_import(module_name, **kwargs)

    assert mod is _json
    assert getattr(_LazyTarget, target_attr) is _json


@pytest.mark.parametrize(
    "bad_alias",
    [
        "TYPE",
        "PRIORITY",
        "serialize",
        "deserialize",
        "can_serialize",
        "can_deserialize",
        "setup_imports",
        "lazy_import",
        "_secret",
    ],
)
def test_lazy_import_rejects_reserved_names(clean_store, bad_alias):
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

    with pytest.raises(ValueError, match="reserved or invalid"):
        _LazyReserved.lazy_import("json", alias=bad_alias)


def test_lazy_import_rejects_double_assignment(clean_store):
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

    _LazyDup.lazy_import("json")
    with pytest.raises(ValueError, match="already set"):
        _LazyDup.lazy_import("sys", alias="json")


def test_setup_imports_accepts_both_signatures(clean_store):
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

    _call_setup_imports(_OneArg, context=None)
    assert _OneArg.called is True

    _call_setup_imports(_TwoArg, context="some-ctx")
    assert _TwoArg.called_with == "some-ctx"
