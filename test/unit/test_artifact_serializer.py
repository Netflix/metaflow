import pytest

from metaflow.datastore.artifacts.serializer import (
    ArtifactSerializer,
    SerializationMetadata,
    SerializedBlob,
    SerializerStore,
)


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
    def serialize(cls, obj):
        blob = obj.encode("utf-8")
        return (
            [SerializedBlob(blob)],
            SerializationMetadata("str", len(blob), "test_high", {}),
        )

    @classmethod
    def deserialize(cls, blobs, metadata, context):
        return blobs[0].decode("utf-8")


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
    def serialize(cls, obj):
        blob = str(obj).encode("utf-8")
        return (
            [SerializedBlob(blob)],
            SerializationMetadata("int", len(blob), "test_low", {}),
        )

    @classmethod
    def deserialize(cls, blobs, metadata, context):
        return int(blobs[0].decode("utf-8"))


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
    def serialize(cls, obj):
        raise NotImplementedError

    @classmethod
    def deserialize(cls, blobs, metadata, context):
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
        def serialize(cls, obj):
            raise NotImplementedError

        @classmethod
        def deserialize(cls, blobs, metadata, context):
            raise NotImplementedError

    # New registration wins
    assert SerializerStore._all_serializers["test_high"] is _ReplacementSerializer
    # But registration order is preserved (only one entry for "test_high")
    assert SerializerStore._registration_order.count("test_high") == 1

    # Restore original for other tests
    SerializerStore._all_serializers["test_high"] = _HighPrioritySerializer


def test_priority_ordering():
    """get_ordered_serializers returns lower PRIORITY first."""
    ordered = SerializerStore.get_ordered_serializers()
    priorities = [s.PRIORITY for s in ordered]
    assert priorities == sorted(priorities)


def test_registration_order_tiebreaker():
    """When PRIORITY is equal, registration order breaks the tie."""
    ordered = SerializerStore.get_ordered_serializers()
    # _SamePrioritySerializer has PRIORITY=100 (same as default)
    # Any other serializer with PRIORITY=100 registered before it should come first
    priority_100 = [s for s in ordered if s.PRIORITY == 100]
    if len(priority_100) > 1:
        order = SerializerStore._registration_order
        indices = [order.index(s.TYPE) for s in priority_100]
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


# ---------------------------------------------------------------------------
# SerializationMetadata tests
# ---------------------------------------------------------------------------


def test_metadata_fields():
    meta = SerializationMetadata(
        type="dict",
        size=1024,
        encoding="pickle-v4",
        serializer_info={"key": "value"},
    )
    assert meta.type == "dict"
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


def test_blob_compress_method_default():
    blob = SerializedBlob(b"data")
    assert blob.compress_method == "gzip"


def test_blob_compress_method_custom():
    blob = SerializedBlob(b"data", compress_method="raw")
    assert blob.compress_method == "raw"


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
